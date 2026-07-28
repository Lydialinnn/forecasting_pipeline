"""One-time backfill of daily_sku_stock_metrics from the three legacy tracker
low-stock logs. (daily_stock is a one-day staging table owned by the live job —
it holds no history and is NOT touched here.) Run LOCALLY (uses your gcloud ADC):

    python backfill_daily_stock.py

Notes:
- The logs only contain low/OOS SKUs, not full inventory scope. That is fine
  for this purpose: stg_low_stock_list uses row presence + thresholds, and a
  SKU absent on a day is simply treated as not low stock.
- stock_in_days is carried DIRECTLY from the files (no order_line_item
  recomputation): STLTH -> STOCK_IN_DAYS, Disposable -> 'STOCK IN DAYS',
  Juice -> 0 when qty <= 0 else null (null never flags downstream;
  qty <= 0 flags via the inventory_qty condition anyway).
  avg_daily_sales_14d is left null for backfilled rows.
- AM/PM rows (e.g. '2025-06-27AM' / '...PM') are deduped to one row per
  sku-day with the MIN qty / MIN stock_in_days (worst case of the day).
- Dates already present in daily_sku_stock_metrics (e.g. written by the live
  Cloud Run job) are NEVER touched — only missing snapshot_dates are
  inserted, so the script is safe to re-run.
"""

import logging

import numpy as np
import pandas as pd
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill_daily_stock")

PROJECT = "valor-sales"
DATASET = "valor_inventory_logs"
METRICS_TABLE = f"{PROJECT}.{DATASET}.daily_sku_stock_metrics"

BASE = "/Users/stlth/Documents/Shopify/Shopify automation/output/Valor_email_prep"
JUICE_CSV = f"{BASE}/Juice_tracker_daily_oos_log.csv"
DISPOSABLE_CSV = f"{BASE}/Valor_Disposable_daily_low_stock_log.csv"
STLTH_CSV = f"{BASE}/STLTH_tracker_daily_low_stock_log.csv"

METRICS_SCHEMA = [
    bigquery.SchemaField("snapshot_date", "DATE"),
    bigquery.SchemaField("sku", "STRING"),
    bigquery.SchemaField("sku_status", "STRING"),
    bigquery.SchemaField("inventory_qty", "INT64"),
    bigquery.SchemaField("avg_daily_sales_14d", "FLOAT64"),
    bigquery.SchemaField("stock_in_days", "FLOAT64"),
]


def parse_snapshot_date(series: pd.Series) -> pd.Series:
    """'2025-06-27AM' / '2025-06-27PM' -> date."""
    return pd.to_datetime(series.astype(str).str[:10], errors="coerce").dt.date


def load_juice() -> pd.DataFrame:
    df = pd.read_csv(JUICE_CSV)
    df.columns = df.columns.str.strip()
    qty = pd.to_numeric(df["SHOPIFY_QTY_merged"], errors="coerce")
    out = pd.DataFrame({
        "snapshot_date": parse_snapshot_date(df["Recording_date"]),
        "sku": df["SKU"].astype(str).str.strip(),
        "product_title": df["Brand"].fillna("").astype(str).str.strip()
            + "-" + df["Flavor"].fillna("").astype(str).str.strip(),
        "sku_status": None,
        "inventory_qty": qty,
        # no stock-cover column in this log: 0 when OOS, else null
        "stock_in_days": np.where(qty <= 0, 0.0, np.nan),
    })
    log.info("Juice log: %s rows", len(out))
    return out


def load_disposable() -> pd.DataFrame:
    df = pd.read_csv(DISPOSABLE_CSV)
    df.columns = df.columns.str.strip()
    out = pd.DataFrame({
        "snapshot_date": parse_snapshot_date(df["Recording_date"]),
        "sku": df["SKU"].astype(str).str.strip(),
        "product_title": df["Product Name"],
        "sku_status": df["PUBLISHED"],
        # NOTE: disposable log uses SHOPIFY_STOCK_merged (not QTY_merged)
        "inventory_qty": pd.to_numeric(df["SHOPIFY_STOCK_merged"], errors="coerce"),
        "stock_in_days": pd.to_numeric(df["STOCK IN DAYS"], errors="coerce"),
    })
    log.info("Disposable log: %s rows", len(out))
    return out


def load_stlth() -> pd.DataFrame:
    df = pd.read_csv(STLTH_CSV, dtype={"VALOR SKU": "str"})
    df.columns = df.columns.str.strip()
    out = pd.DataFrame({
        "snapshot_date": parse_snapshot_date(df["Recording_date"]),
        "sku": df["VALOR SKU"].astype(str).str.strip(),
        "product_title": df["Product Title"],
        "sku_status": None,
        "inventory_qty": pd.to_numeric(df["SHOPIFY_QTY_merged"], errors="coerce"),
        "stock_in_days": pd.to_numeric(df["STOCK_IN_DAYS"], errors="coerce"),
    })
    log.info("STLTH log: %s rows", len(out))
    return out


def build_backfill() -> pd.DataFrame:
    df = pd.concat([load_juice(), load_disposable(), load_stlth()], ignore_index=True)

    n0 = len(df)
    df = df[
        df["snapshot_date"].notna()
        & df["inventory_qty"].notna()
        & df["sku"].notna()
        & ~df["sku"].str.lower().isin(["", "none", "nan"])
    ]
    log.info("Dropped %s rows with missing date/qty/sku", n0 - len(df))

    # AM/PM + cross-file dedupe: one row per sku-day, worst case of the day
    df = (
        df.groupby(["sku", "snapshot_date"], as_index=False)
        .agg(
            product_title=("product_title", "first"),
            sku_status=("sku_status", "first"),
            inventory_qty=("inventory_qty", "min"),
            stock_in_days=("stock_in_days", "min"),  # min ignores NaN unless all NaN
        )
    )
    df["inventory_qty"] = df["inventory_qty"].round().astype(int)
    log.info("Backfill candidate: %s rows, %s dates (%s → %s)",
             len(df), df["snapshot_date"].nunique(), df["snapshot_date"].min(), df["snapshot_date"].max())
    return df


def existing_dates(client: bigquery.Client, table: str) -> set:
    rows = client.query(f"select distinct snapshot_date from `{table}`").to_dataframe()
    return set(rows["snapshot_date"].tolist())


def load_missing_dates(client: bigquery.Client, df: pd.DataFrame, table: str,
                       columns: list[str], schema: list[bigquery.SchemaField]) -> None:
    skip = existing_dates(client, table)
    log.info("%s: %s dates already present — skipped", table, len(skip))

    out = df[~df["snapshot_date"].isin(skip)][columns].copy()
    if out.empty:
        log.info("%s: nothing new to backfill.", table)
        return

    log.info("Loading %s rows (%s new dates) into %s", len(out), out["snapshot_date"].nunique(), table)
    client.load_table_from_dataframe(
        out, table,
        job_config=bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND"),
    ).result()


def main() -> None:
    client = bigquery.Client(project=PROJECT)
    df = build_backfill()

    df["avg_daily_sales_14d"] = np.nan  # unknown for backfilled history
    load_missing_dates(
        client, df, METRICS_TABLE,
        ["snapshot_date", "sku", "sku_status", "inventory_qty", "avg_daily_sales_14d", "stock_in_days"],
        METRICS_SCHEMA,
    )

    log.info("Done. Verify: select snapshot_date, count(*) from `%s` group by 1 order by 1", METRICS_TABLE)


if __name__ == "__main__":
    main()
