"""Daily SKU stock snapshot + stock-cover metrics. Cloud Run Job.

Replaces the placeholder low-stock source for the forecasting project:

1. Pulls ALL SKUs' real-time inventory from Shopify (products + inventory_levels)
   and stages it in `daily_stock` — a ONE-DAY staging table, truncated on every
   run (kept only so today's raw snapshot is inspectable; no downstream input).
2. Computes each SKU's trailing 14-day average daily sales straight from the RAW
   `order_line_item` table (NOT from the dbt models — those only refresh weekly)
   with the same exclusion filters as fct_daily_sku_sales, then writes
   `daily_sku_stock_metrics`: inventory_qty, avg_daily_sales_14d, stock_in_days.

No low-stock filter is applied here — the table carries ALL SKUs so ops can
query any cover threshold. The forecasting flag
(qty <= 0 OR stock_in_days <= 1) is applied in dbt: stg_low_stock_list.

`daily_sku_stock_metrics` is the ONLY history table: appends daily, replaces
the same-day partition on re-run (vintage pattern, same as the forecast tables).

Env vars:
    GCP_PROJECT        default 'valor-sales'
    BQ_DATASET         default 'valor_inventory_logs'
    LINE_ITEM_TABLE    default 'valor-sales.valor_shopify_line_item.order_line_item'
    SHOPIFY_SECRET_ID  Secret Manager id holding the Shopify token
                       (or inline via SHOPIFY_ACCESS_TOKEN)
    API_SHOP           default 'valordistributions'
    API_VERSION        default '2025-04'
    SALES_WINDOW_DAYS  default '14'
"""

import json
import logging
import os
from zoneinfo import ZoneInfo
from datetime import datetime

import pandas as pd
from google.cloud import bigquery, secretmanager

from shopify_client import (
    extract_inventory_data,
    get_all_product_inventory,
    get_inventory_levels,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("daily_stock_log")

PROJECT = os.environ.get("GCP_PROJECT", "valor-sales")
DATASET = os.environ.get("BQ_DATASET", "valor_inventory_logs")
LINE_ITEM_TABLE = os.environ.get(
    "LINE_ITEM_TABLE", "valor-sales.valor_shopify_line_item.order_line_item"
)
API_SHOP = os.environ.get("API_SHOP", "valordistributions")
API_VERSION = os.environ.get("API_VERSION", "2025-04")
SALES_WINDOW_DAYS = int(os.environ.get("SALES_WINDOW_DAYS", "14"))
TZ = ZoneInfo("America/Toronto")

STOCK_TABLE = f"{PROJECT}.{DATASET}.daily_stock"
METRICS_TABLE = f"{PROJECT}.{DATASET}.daily_sku_stock_metrics"

STOCK_SCHEMA = [
    bigquery.SchemaField("snapshot_date", "DATE"),
    bigquery.SchemaField("sku", "STRING"),
    bigquery.SchemaField("product_title", "STRING"),
    bigquery.SchemaField("sku_status", "STRING"),
    bigquery.SchemaField("inventory_qty", "INT64"),
]


def shopify_token() -> str:
    if os.getenv("SHOPIFY_ACCESS_TOKEN"):
        return os.environ["SHOPIFY_ACCESS_TOKEN"]
    secret_id = os.environ["SHOPIFY_SECRET_ID"]
    name = (
        secret_id if secret_id.startswith("projects/")
        else f"projects/{PROJECT}/secrets/{secret_id}/versions/latest"
    )
    client = secretmanager.SecretManagerServiceClient()
    payload = client.access_secret_version(request={"name": name}).payload.data.decode("utf-8").strip()
    try:  # secret may be JSON ({"Shopify_store_password": ...}) or the raw token
        parsed = json.loads(payload)
        if isinstance(parsed, dict):
            return parsed.get("Shopify_store_password") or parsed.get("access_token")
    except json.JSONDecodeError:
        pass
    return payload


def fetch_inventory_snapshot() -> pd.DataFrame:
    """All SKUs' real-time inventory, one row per sku (summed across variants/locations)."""
    headers = {"X-Shopify-Access-Token": shopify_token()}

    products = get_all_product_inventory(API_SHOP, API_VERSION, headers)
    inventory_df = pd.DataFrame(extract_inventory_data(products))
    inventory_df["variant_sku"] = inventory_df["variant_sku"].fillna("none").astype(str)
    inventory_df = inventory_df[inventory_df["variant_sku"] != "none"]
    inventory_df = inventory_df.drop_duplicates(subset=["inventory_item_id"])

    item_ids = inventory_df["inventory_item_id"].dropna().unique().tolist()
    log.info("Fetching real-time inventory levels for %s items", len(item_ids))
    levels_df = pd.DataFrame(get_inventory_levels(API_SHOP, API_VERSION, headers, item_ids))

    if not levels_df.empty:
        actual = levels_df.groupby("inventory_item_id")["available"].sum().reset_index()
        actual.rename(columns={"available": "inventory_qty"}, inplace=True)
        inventory_df = inventory_df.merge(actual, on="inventory_item_id", how="left")
        inventory_df["inventory_qty"] = (
            inventory_df["inventory_qty"]
            .fillna(inventory_df["legacy_inventory_quantity"])
            .fillna(0)
            .astype(int)
        )
    else:
        log.warning("inventory_levels returned nothing; falling back to legacy quantities")
        inventory_df["inventory_qty"] = inventory_df["legacy_inventory_quantity"].fillna(0).astype(int)

    snapshot = (
        inventory_df.groupby("variant_sku")
        .agg(
            product_title=("product_title", "first"),
            sku_status=("sku_status", "first"),
            inventory_qty=("inventory_qty", "sum"),
        )
        .reset_index()
        .rename(columns={"variant_sku": "sku"})
    )
    snapshot["snapshot_date"] = datetime.now(TZ).date()
    return snapshot[["snapshot_date", "sku", "product_title", "sku_status", "inventory_qty"]]


def load_snapshot(client: bigquery.Client, snapshot: pd.DataFrame) -> None:
    """One-day staging: TRUNCATE + load. daily_stock never accumulates history."""
    client.load_table_from_dataframe(
        snapshot, STOCK_TABLE,
        job_config=bigquery.LoadJobConfig(schema=STOCK_SCHEMA, write_disposition="WRITE_TRUNCATE"),
    ).result()
    log.info("Wrote %s rows to %s (truncated)", len(snapshot), STOCK_TABLE)


def build_metrics(client: bigquery.Client) -> None:
    """Join today's snapshot to trailing sales from the RAW line-item table.

    Sales filters MUST stay in sync with the dbt project:
    macros/global_order_exclusions.sql + the forecasting-only exclusions in
    fct_daily_sku_sales.sql. The product-scope regex (forecasting_product_regex)
    is intentionally NOT applied — this table covers all SKUs; scope filtering
    happens downstream when fct_daily_sku_sales joins by sku.
    """
    client.query(f"""
        create table if not exists `{METRICS_TABLE}` (
            snapshot_date date,
            sku string,
            sku_status string,
            inventory_qty int64,
            avg_daily_sales_14d float64,
            stock_in_days float64
        )
        partition by snapshot_date
        cluster by sku
    """).result()

    query = f"""
        declare snapshot_d date default (select max(snapshot_date) from `{STOCK_TABLE}`);

        delete from `{METRICS_TABLE}` where snapshot_date = snapshot_d;

        insert into `{METRICS_TABLE}`
        with sales_14d as (
            select
                o.sku,
                sum(o.net_qty) / {SALES_WINDOW_DAYS} as avg_daily_sales_14d
            from `{LINE_ITEM_TABLE}` o
            -- order_date is a DATE partition column: filter it RAW (no date() wrapper)
            -- so BigQuery prunes to the {SALES_WINDOW_DAYS}-day window
            where o.order_date >= date_sub(snapshot_d, interval {SALES_WINDOW_DAYS} day)
              and o.order_date < snapshot_d
              -- === keep in sync: macros/global_order_exclusions.sql ===
              and o.cancel_at is null
              and not regexp_contains(lower(coalesce(o.product_title, 'none')),
                    r'excise|tax|fee|custom item|flavour menu|\\brma\\b|return variance')
              and not regexp_contains(coalesce(o.sku, 'none'), r'-STH$')
              and not regexp_contains(coalesce(o.customer_name, 'none'), r'^STLTH')
              and coalesce(o.sku, 'none') != 'none'
              and o.net_qty != 0
              -- === keep in sync: forecasting-only exclusions in fct_daily_sku_sales.sql ===
              and not regexp_contains(lower(coalesce(o.product_title, 'none')),
                    r'variety carton|tester tips|variety stand|marketing')
              and not regexp_contains(lower(coalesce(o.customer_name, 'none')),
                    r'lucka lepage|lk hardware')
            group by 1
        )

        select
            s.snapshot_date,
            s.sku,
            s.sku_status,
            s.inventory_qty,
            coalesce(f.avg_daily_sales_14d, 0) as avg_daily_sales_14d,
            safe_divide(s.inventory_qty, f.avg_daily_sales_14d) as stock_in_days
        from `{STOCK_TABLE}` s
        left join sales_14d f on f.sku = s.sku
        where s.snapshot_date = snapshot_d;
    """
    client.query(query).result()
    log.info("Metrics written to %s", METRICS_TABLE)


def main() -> None:
    client = bigquery.Client(project=PROJECT)
    snapshot = fetch_inventory_snapshot()
    if snapshot.empty:
        log.error("Empty inventory snapshot — aborting without write.")
        raise SystemExit(1)
    load_snapshot(client, snapshot)
    build_metrics(client)
    log.info("Done.")


if __name__ == "__main__":
    main()
