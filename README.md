# Valor Weekly Forecasting dbt Project

Standalone dbt (BigQuery) project for **SKU-level WEEKLY demand forecasting** on **ordered** (not fulfilled) sales. Deliberately separate from `valor_margin_dbt` — the only shared pieces are listed under [External dependencies](#external-dependencies-keep-in-sync-manually).

The pipeline: prepare a gap-free daily SKU sales dataset with low-stock imputation → **aggregate to Mon-start ISO weeks (complete weeks only)** → run three candidate forecast models (TimesFM, ARIMA_PLUS, Kalman) plus a naive 4-week-average baseline on the weekly series → append forecast vintages → evaluate on holdout weeks by **counting extreme weekly misses** → select a winner per SKU (must beat naive).

The purpose is the **10-week (order_horizon_weeks) BASE demand** per SKU — the expected *normal* run-rate demand, not daily shape. It is deliberately NOT a purchase-order quantity: known future promos, launches or other one-offs are added on top of this base by the buyer. Hence the weekly grain, the spike winsorization, and the variance-focused evaluation.

Builds into BigQuery dataset **`valor-sales.valor_weekly_forecasting`** (region `northamerica-northeast2`).

---

## Project Structure

```
VALOR_WEEKLY_FORECASTING/
├── models/
│   ├── staging/
│   │   ├── _sources.yml                     ← raw Shopify orders, inventory log, valor_margin view, external kalman table
│   │   ├── stg_valor_shopify__order_line_item.sql   ← lean read of raw order lines (unit_price kept for availability only)
│   │   ├── stg_valor_margin__product_category.sql   ← passthrough of valor_margin_dbt.int_product_category
│   │   └── stg_low_stock_list.sql           ← low-stock flags from daily_sku_stock_metrics (Cloud Run job)
│   │
│   ├── intermediate/
│   │   └── int_sku_description.sql          ← 1 row/SKU labels: sku_description, _extracted_unit_conversion,
│   │                                          brand_category_key (latest record per SKU; display only, never a series ID)
│   │
│   └── forecasting/
│       ├── fct_daily_sku_sales.sql          ← DAILY imputation layer: scope filters, gap-free spine,
│       │                                      low-stock imputation (models do not read this directly)
│       ├── fct_weekly_sku_sales.sql         ← shared MODEL INPUT: Mon-start ISO weekly sums,
│       │                                      complete weeks only, eligibility flags
│       ├── forecast_timesfm.sql             ← AI.FORECAST TimesFM 2.0 (weekly net_qty_unconstrained)
│       ├── forecast_arima.sql               ← ARIMA_PLUS, weekly; CREATE MODEL via pre_hook
│       ├── forecast_naive_baseline.sql      ← last-4-complete-weeks average; mandatory benchmark floor
│       ├── forecast_results_unioned.sql     ← view; + kalman source when include_kalman=true;
│       │                                      applies the CENTRAL per-SKU output cap to all models
│       ├── forecast_evaluation_weeks.sql    ← sku × week × model holdout detail, is_extreme_week flag,
│       │                                      low-stock weeks excluded
│       ├── forecast_evaluation.sql          ← per sku × model: n_extreme_weeks (PRIMARY),
│       │                                      order-window total error, weekly WAPE
│       ├── forecast_model_winner.sql        ← per-SKU winner by fewest extreme weeks (must beat naive)
│       ├── forecasting_total.sql            ← BASE demand table: winner's order-window total per SKU (units + cartons)
│       └── forecast_display.sql             ← one-table BI source (Tableau): weekly actuals × models +
│                                              latest-vintage forecasts with bounds; model_name = section filter
│
├── macros/
│   ├── global_order_exclusions.sql          ← single definition of the global exclusion filters
│   └── valor_brand_base.sql                 ← copied from valor_margin_dbt
│
├── tests/                                   ← singular tests: daily + weekly uniqueness, no gaps,
│                                              non-negative imputed demand, Monday week starts
│
├── cloud_run/
│   ├── kalman_forecast/                     ← Cloud Run Job (valor-kalman-forecast, deployed):
│   │                                          statsmodels Kalman local linear trend on weekly sums
│   └── daily_stock_log/                     ← Cloud Run Job (daily): Shopify inventory snapshot →
│                                              daily_stock + daily_sku_stock_metrics (14-day avg
│                                              sales from raw order_line_item, stock cover); feeds
│                                              stg_low_stock_list
│
├── dbt_project.yml                          ← config + forecasting vars
└── profiles.yml                             ← dev: OAuth / prod: service-account impersonation
```

### Model flow

```
stg_valor_shopify__order_line_item ──┬──> int_sku_description <── stg_valor_margin__product_category
                                     │         (labels; joined onto outputs for charts)
                                     │
                                     └──> fct_daily_sku_sales <── stg_low_stock_list
                                                │  (daily low-stock imputation)
                                       fct_weekly_sku_sales
                                                │  (Mon-start weekly sums, complete weeks only)
              ┌──────────────────┬──────────────┼────────────────────┐
      forecast_timesfm     forecast_arima   forecast_naive   [forecast_kalman — Cloud Run job]
              └──────────────────┴──────────────┴────────────────────┘
                                     │
                        forecast_results_unioned (view)
                                     │
                     forecast_evaluation_weeks (sku × week detail)
                                     │
                       forecast_evaluation (extreme counts)
                                     │
                          forecast_model_winner
```


---

## Key Conventions

- **Forecast series ID = `sku`** (sku_base + stamp_type grain). 
- **`int_sku_description` is display-only**: latest `product_title`+`variant` per SKU, uniform across history. Joined onto outputs for chart labels; never fed to models.
- **Two launch-related windows, deliberately different lengths — they do different jobs:**
  - `launch_weeks` (12) = **no OOS fill-in** for a SKU's first 12 weeks. Out-of-stock days in that period keep their raw value instead of being estimated, because early-life demand is too noisy to reconstruct.
  - `winsor_ref_exclude_weeks` (2) = the first 2 complete weeks don't count when **calculating** the p75 reference (they're the channel-fill weeks, so letting them set the ceiling would defeat the purpose). They are still **subject to** the ceiling — see the next bullet. Kept at 2, not 12, so that SKUs younger than 12 weeks still get a ceiling at all.
  - Why 2 is enough: with OOS fill-in switched off for 12 weeks, weeks 3–12 hold real sales figures. Previously the fill-in could copy a launch spike forward into weeks 3–8, inflating those weeks and therefore the ceiling too — that's no longer possible, so the ceiling can be calculated from week 3 onward safely.
- **Imputation stays DAILY, modeling is WEEKLY**: `fct_daily_sku_sales` fills a low-stock (censored) day with the **trailing average daily rate over NON-low-stock days** — all days in the window, *not* same-weekday (same-weekday averaging used only ~4 samples, so one spiky weekday skewed it; all-days gives 28/56 samples). The value is `greatest(28-day rate, 56-day rate, net_qty_raw)`: taking the max means a declining 28-day window can't under-impute below the longer view, and the result is always **floored at raw** — true demand on a censored day is at least what sold. (Both windows are coalesced to 0 first, since BigQuery `GREATEST` returns NULL if any argument is NULL — so an all-censored window falls back to raw.) **No imputation during the launch window** (`launch_weeks`): those weeks stay raw — launch demand is too noisy to reconstruct, and imputing there echoes the launch spike into later weeks. Stored as `net_qty_unconstrained`; `fct_weekly_sku_sales` sums both columns into Mon-start ISO weeks. **All models train on the weekly `net_qty_unconstrained` sums** — the ARIMA XREG regressor is gone.
- **Complete weeks only**: the in-progress trailing week and any partial launch week are dropped so every row is a uniform 7-day total. Weekly series stay gap-free (required by AI.FORECAST frequency inference).
- **Low-stock flag = true OOS only**: a `(sku, date)` row is in `stg_low_stock_list` when `inventory_qty <= 0`. (The old `stock_in_days <= 1` velocity condition was dropped — during high-velocity launches it fired on healthy, well-selling days, so the imputation overwrote real demand. Only actual zero-stock days count as censored now.) Weekly carries `low_stock_days` (0–7).
- **Spike ceiling** (`fct_weekly_sku_sales`): launch weeks are genuinely high (channel fill), so a ceiling is applied to the model input: any week above `input_winsor_multiple` (3) × the **75th percentile** of the reference weeks is reduced down to that ceiling. Reference weeks = the SKU's complete weeks after `winsor_ref_exclude_weeks` (2), excluding heavily out-of-stock weeks. Two details that matter:
  - `winsor_ref_exclude_weeks` controls which weeks *define* the ceiling, **not** which weeks get reduced — the ceiling is applied to every week, weeks 1–2 included.
  - **75th percentile — not max, not median.** Under a `max` reference the ceiling is unreachable for any week inside the reference set (such a week is ≤ the max by definition), so only weeks 1–2 could ever be reduced. The median went too far the other way: measured on live data it removed **~19% of real units from the smallest SKUs vs ~2% from the largest**, and left ~24% of SKUs (median = 0, intermittent sellers) with no ceiling at all. The 75th percentile removes a near-flat **~2–5% across every SKU size**, so it trims launch spikes *and* mid-life spikes toward typical demand without penalising small SKUs.
  - **Heavily censored weeks are excluded from the reference** (`low_stock_days > scoring_max_low_stock_days`): an OOS week reads near zero — with no imputation at all inside `launch_weeks` — and would drag the reference down, over-tightening the ceiling for legitimate weeks. Same threshold as evaluation scoring.
  - **A zero reference disables the ceiling** (`nullif(p75, 0)`): a very sparse SKU can have a p75 of 0, and a ceiling of 0 would reduce its entire history to zeros. Zero is treated as "no usable reference" → no ceiling, values pass through.

  `net_qty_raw` is never modified.

  | SKU age (complete weeks) | behavior |
  |---|---|
  | ≤ 2 | pass-through (no reference week yet) |
  | ≥ 3 | reduce any week above `input_winsor_multiple` × p75(weeks 3+) down to that ceiling |

- **Central output cap** (`forecast_results_unioned`): every model's `forecast_value` is capped at `output_cap_multiple` (3) × the SKU's p75 reference week — the same reference the input ceiling uses. It lives in the union **view**, not inside each model, so (a) no model can be the exception (Kalman runs in a separate Cloud Run job and previously had no cap, letting its trend extrapolate past the ceiling straight into the base-demand number), (b) everything downstream — evaluation, winner selection, `forecasting_total`, `forecast_display` — inherits it, and (c) because it's a view, changing the multiple re-caps everything at read time with no model re-runs. `lower_bound`/`upper_bound` are deliberately left uncapped: they carry each model's genuine uncertainty, which the coverage and interval-width diagnostics depend on.

  Historical note on why an over-long exclusion was low-risk anyway: a SKU under ~20 weeks has no winner row, so `forecasting_total` takes its number from `fallback_model` (TimesFM) or bootstrap — **ARIMA/Kalman output is never consumed for young SKUs** — and by the time a SKU *is* evaluated (~20 wks) it's past `launch_weeks` and already capped. The gap only affected (a) charts for young SKUs and (b) TimesFM's training input having no ceiling, and TimesFM doesn't extrapolate trend like ARIMA, so it's the least spike-sensitive of the three.

- **Scoring is HYBRID**: forecasts are graded against the **unconstrained** weekly actual (raw + imputed censored days) so lightly censored weeks stay scoreable, but weeks with `low_stock_days > scoring_max_low_stock_days` (default 3) are excluded — the imputation ≈ the naive formula, so grading against a mostly-imputed "actual" would hand naive artificial wins.
- **Forecast tables append vintages**: incremental on `forecast_run_date` (insert_overwrite = same-day rerun replaces that day's vintage only). "Current forecast" = `where forecast_run_date = max(...)`.
- **Eligibility**: ≥ 4 complete weeks history (`has_min_history`), any sales in last 9 weeks (`is_active`). SKU lifecycle in `forecasting_total`: <4 complete wks = `bootstrap_weekly_avg` run-rate (sum of sales ÷ iso-weeks available incl. partial × order_horizon_weeks); 4–19 wks = all models forecast, total uses `fallback_model` (timesfm_2_0, `is_fallback_model = true`); ~20 wks = scoreable by the next evaluation run (~10 training + 10 holdout), after which its winner drives the total.
- **Evaluation = variance-focused, not total-focused**: a week is an **extreme miss** when abs pct error > `extreme_ape_threshold` (50%); when actual = 0, when predicted ≥ `extreme_zero_week_units` (5). prefer the model with the fewest big weekly misses over the one closest on the order-window total.
- **Winner rule**: fewest `n_extreme_weeks` per SKU → tiebreak `total_abs_pct_err` (order-window total) → weekly `wape`. Min `min_scored_weeks` (6) scored holdout weeks; candidate must beat `naive_4wk_avg` on (extremes, then total err) for that SKU, else the SKU keeps naive.

## Forecasting vars (`dbt_project.yml`)

| var | default | meaning |
|---|---|---|
| `forecast_holdout_weeks` | 10 | evaluation mode (= order window); set **0** for production runs. NOTE: the deployed pipeline's evaluation default lives in `run_pipeline.sh` — keep in sync |
| `order_horizon_weeks` | 10 | base-demand window = forecast horizon = evaluation window length |
| `forecast_min_history_weeks` | 4 | forecasting eligibility (evaluation effectively needs ~20 wks: 10 holdout + ~10 training) |
| `fallback_model` | timesfm_2_0 | used in forecasting_total for SKUs too young to have a winner row |
| `model_divergence_low` | 0.25 | forecasting_total: model-disagreement ratio ≤ this = "models agree" |
| `model_divergence_high` | 0.75 | above this = "models disagree" |
| `launch_weeks` | 12 | no-imputation window: `fct_daily` leaves low-stock days as raw for the SKU's first 12 weeks |
| `winsor_ref_exclude_weeks` | 2 | initial complete weeks excluded when finding the peak that anchors the winsor ceiling (`fct_weekly`) and the ARIMA cap — short so young SKUs still get a ceiling |
| `output_cap_multiple` | 3 | **central** output cap in `forecast_results_unioned`: every model's `forecast_value` capped at this × the SKU's p75 reference week. Applied in a view, so changing it re-caps without re-running models. Bounds deliberately uncapped |
| `input_winsor_multiple` | 3 | `fct_weekly_sku_sales`: `net_qty_unconstrained` capped at this × the **75th percentile** of the reference weeks (trims launch + mid-life spikes; `net_qty_raw` untouched; SKUs with no usable reference pass through) |
| `forecast_activity_window_weeks` | 9 |`is_active`, can be discontinued so get no forecasts|
| `min_scored_weeks` | 6 | winner selection: min scored holdout weeks (allows up to 4 excluded weeks), each scored weeks should have no more than `scoring_max_low_stock_days`  |
| `extreme_ape_threshold` | 0.5 | weekly miss > 50% = extreme |
| `extreme_zero_week_units` | 5 | extreme floor when actual = 0 |
| `scoring_max_low_stock_days` | 3 | weeks with more low-stock days than this are (a) unscoreable in evaluation and (b) excluded from the p75 reference that anchors the winsor ceiling / ARIMA cap — in both cases because a censored week isn't a trustworthy demand observation |
| `forecasting_brand_category_list` | 27 brand keys | product scope: EXACT whitelist of int_sku_description.brand_category_key values (case-insensitive equality, no substring matching) — broaden by adding list entries, not SQL |
| `include_kalman` | **true** | forecast_kalman written by the valor-kalman-forecast Cloud Run job |


---

## Interpreting the evaluation metrics (`forecast_evaluation`)

**Point-accuracy metrics — how close `forecast_value` landed. These drive winner selection.**

| metric | meaning | notes |
|---|---|---|
| `n_extreme_weeks` | count of holdout weeks with a big miss (APE > 50%; if actual = 0, forecast ≥ 5 units) | PRIMARY ranking metric: fewest big misses = most reliable |
| `total_abs_pct_err` | abs error of the order-window TOTAL vs actual total | 1st tiebreaker — read alongside `total_actual` |
| `wape` | sum of weekly abs errors ÷ sum of actuals | 2nd tiebreaker; weekly-shape accuracy. Unlike total error, over/under misses don't cancel |
| `mae` / `rmse` / `mape` | absolute-unit and per-week error views | reference only; rmse punishes single huge misses hardest |

**Interval metrics — whether the model's stated 90% band `[lower_bound, upper_bound]` is honest. Diagnostics only; NOT used in winner selection.**

| metric | meaning |
|---|---|
| `coverage_rate` | share of scored holdout weeks where the actual fell inside the band. Target ≈ 0.90 |
| `median_rel_interval_width` | median of weekly (band width ÷ forecast) — how much uncertainty the model claims, normalized by the SKU's sales volume so SKUs of different selling category are comparable |

Read them together:

| coverage | width | verdict |
|---|---|---|
| ≈ 0.90 | narrow | genuinely sharp — bands trustworthy |
| ≪ 0.90 | narrow | OVERCONFIDENT — model misjudges its own errors; treat its bounds as decoration; mild misspecification warning |
| ≈ 0.90 (or higher) | wide | honest but vague — the SKU is volatile; point forecast right on average, buffer accordingly |

Key distinctions, in plain terms:

1. **Point metrics judge the demand number; interval metrics judge the risk statement
   around it.** The base-demand table (`forecasting_total`) is built by summing `forecast_value`
   only — `lower_bound`/`upper_bound` are never read, so no purchase quantity depends
   on them. A model with accurate forecasts but dishonest bands is therefore still a
   legitimate winner: the flaw sits in a column nothing consumes.

2. **When would coverage start to matter for winner selection?** Only if purchasing
   policy ever changes to act on the bounds — e.g. "order up to `upper_bound` to cover
   90% of demand scenarios." At that point a model claiming 90% but delivering 60%
   coverage would cause silent under-buffering, so the winner rule would need a
   calibration gate (e.g. coverage >= 0.85). Until then, coverage is a health
   indicator, not a gate.

3. **All of these metrics describe ONE past 10-week window — none promise the next
   one.** A model can win a single window by luck (one promo, one calm month). The
   defense is repetition, not a better metric: every evaluation run tests a fresh
   window and appends its winners to `winner_history`. A model that wins across
   several cycles is genuinely better; one that won once and never again was noise.

### Production confidence: `model_divergence` (in `forecasting_total`)

The metrics above need actuals, so they only exist after an evaluation run. In
production (holdout 0) there are no actuals yet — but all four models still
produce forecasts, so their **agreement with each other** is a signal you can
read immediately. `model_divergence_ratio` = `(max − min) / mean` of the
candidate models' order-window totals for a SKU (0 = identical, 1 = the spread
equals the average forecast); `model_divergence` buckets it into low / medium /
high via `model_divergence_low` (0.25) and `model_divergence_high` (0.75).

It measures **agreement, not correctness** — all four models could agree and all
be wrong — so it's a "how much to trust this one number" flag for buyers, not a
model-quality metric. It pairs with the winner's historical extreme rate:

- `winner_extreme_rate` asks "**historically, could any model forecast this SKU?**"
- `model_divergence` asks "**right now, do the models even agree on it?**"

A SKU can be low-extreme historically but high-divergence this week (e.g. a
recent trend the models extrapolate differently) — both are worth a second look
before ordering. (Bootstrap SKUs have only one model, so they show
`n/a (single model)`.)

---

## Findings: testing-window sensitivity analysis (2026-07-17)

Method: `run_holdout_sweep.sh` re-ran the full evaluation (all 4 models incl.
Kalman) for holdout lengths 8–15 weeks, snapshotting per-SKU winners into
`winner_holdout_sweep` (+ full per-model metrics into `evaluation_holdout_sweep`).
NOT cross-validation: the test windows share an endpoint and are nested, and the
training cutoff moves with the holdout — read as "does the conclusion survive the
evaluation design?", not as an averaged error estimate.

Results:

- **TimesFM leads volume share at every setting (8–15 wks)** — it wins the SKUs
  that carry the most units even where naive wins more SKUs by count (holdout 8:
  naive 102 SKUs / 28.9% volume vs TimesFM 91 / 30.7%). The winner is NOT an
  artifact of one evaluation design → confirms `fallback_model: timesfm_2_0`
  for unevaluated SKUs.
- **Naive decays monotonically with window length** (102 SKUs / 28.9% at 8 →
  53 / 13.6% at 15): recency is its only asset; a flat 4-week average cannot
  survive longer windows. Consistent with its role as benchmark floor.
- **Kalman and ARIMA are steady mid-pack** — they earn per-SKU wins without
  dominating; the per-SKU winner system captures that value.
- Caveat: longer-holdout settings also train on older data, so naive's decay
  partly reflects training staleness, not window length alone.

On `winner_extreme_rate` (winner's extreme weeks ÷ scored weeks): the winner has
the lowest count of the four by construction, so a HIGH rate means the SKU hit
its **forecastability ceiling** — no candidate model avoids big weekly misses.
That signals inherently volatile demand or an input-data problem (unflagged OOS,
promos), and the response is buffering / manual review / data investigation, not
model changes. SKUs with a high winner rate across ALL sweep settings are
confirmed-unforecastable rather than unlucky in one window.

Empirical result (2026-07-17 sweep), in plain terms:

- On a typical SKU, the winning model has a big miss (>50% off) in only ~1–2
  weeks out of 10 (avg winner extreme rate 0.12–0.20; 0.13 at the official
  10-week window). Winners are doing their job.
- "Hard-to-forecast SKUs" = SKUs where even the BEST model misses badly in half
  or more of the scored weeks (winner extreme rate ≥ 0.5). These were only
  3.5–4.6% of SKUs at windows ≤ 13 weeks, rising to 11.7% at 14–15-week windows
  (expected: those settings train on staler data and score older weeks).
- **No SKU was hard-to-forecast at every window setting** — every SKU is
  forecastable under at least one evaluation design — so no "unreliable, buffer
  manually" flag is needed in the base-demand table yet. Re-check this as `winner_history`
  accumulates evaluation cycles.

Ongoing robustness: instead of retrospective time-series CV (which would test
models on artificially short training histories that production never uses),
each periodic evaluation run appends its winners to `winner_history` — a
forward-accumulating set of full-training-size test windows for judging model
consistency over time.

---

## Deployment (Cloud Run)

| piece | type | schedule (Toronto) | deploy |
|---|---|---|---|
| `valor-daily-inventory-log` | job | daily 6:00 | `cloud_run/daily_stock_log/deploy.sh` |
| `valor-kalman-forecast` | job | none (triggered by the pipeline) | `cloud_run/kalman_forecast/deploy.sh` |
| `valor-weekly-forecast-pipeline` | job | Mon 8:00 (production) + daily 9:00 (sku_refresh) | `./deploy_pipeline.sh` |
| `valor-forecasting-docs` | service | manual re-publish | `./deploy_docs.sh` |

Docs (lineage + model documentation):
<https://valor-forecasting-docs-456816746692.northamerica-northeast2.run.app>

Tableau dashboard (weekly actuals vs each model's forecast, reads `forecast_display`):
<https://prod-ca-a.online.tableau.com/#/site/canadianventures/views/forecasting_model/weekly_display>

### The weekly pipeline job

`run_pipeline.sh` (the container entrypoint) chains, per `PIPELINE_MODE`:

- **production** (default; Monday scheduler): holdout **0** — dbt builds staging →
  `fct_weekly_sku_sales`, triggers the Kalman job and waits, then builds the
  forecast models + `forecasting_total` + `forecast_display`. Evaluation/winner
  models are SKIPPED (no holdout = no actuals to score; rebuilding them would
  wipe the winner mapping).
- **evaluation** (manual, occasional): holdout **10** — full rebuild including
  `forecast_evaluation_weeks` → `forecast_evaluation` → `forecast_model_winner`,
  then appends the winners to **`winner_history`** (tagged with run date +
  holdout) so model consistency can be judged across evaluation cycles —
  each cycle is a fresh 10-week test window at full training size, i.e. a
  forward-accumulating alternative to retrospective time-series CV.
- **sku_refresh** (daily scheduler): incremental upsert of `int_sku_description` only.

### Occasional evaluation run (refresh the winner mapping)

```bash
gcloud run jobs execute valor-weekly-forecast-pipeline \
  --region=northamerica-northeast2 --project=valor-sales \
  --update-env-vars PIPELINE_MODE=evaluation --wait

# then restore current production vintages (or wait for Monday's schedule,
gcloud run jobs execute valor-weekly-forecast-pipeline \
  --region=northamerica-northeast2 --project=valor-sales --wait
```

`--update-env-vars` on execute is per-execution only — scheduled runs stay production.

### Changing the forecasting scope (`forecasting_brand_category_list`)

The var lives in `dbt_project.yml`, which is baked into the pipeline image —
local edits do nothing until redeployed. Procedure:

1. Edit the var locally; sanity-check the SKU list:
   `select distinct brand_category_key from valor_weekly_forecasting.int_sku_description order by 1;`
2. `./deploy_pipeline.sh` (rebuilds the image with the new var)
3. Run an **evaluation** execution (command above) — new-scope SKUs need winners
   selected before production forecasts can use them
4. Either run a production execution right after, or wait for Monday's schedule
5. `./deploy_docs.sh` to refresh the published docs (optional)

New SKUs need ≥ `forecast_min_history_weeks` (4) complete weeks of history and
recent activity to be forecast by the models — below that, recently-selling SKUs
still get a `bootstrap_weekly_avg` run-rate total in forecasting_total; from
4 weeks they get TimesFM-fallback totals, and a winner after ~20 weeks
(~10 training + 10 holdout, applied at the next evaluation run).


---

## External dependencies (keep in sync manually)

1. **`valor_margin_dbt.int_product_category`** — sourced as a view for `brand_category_key`. If valor_margin renames/drops it, update `_sources.yml`.
2. **Global exclusion filters** — defined once in `macros/global_order_exclusions.sql`, copied from `valor_margin_dbt/int_fulfilled_sales`. If the rules change there, mirror them in the macro.
3. **`valor_brand_base` macro** — copied from valor_margin_dbt.

## Deferred / TODOs

- Rolling backtest (true time-series CV) deliberately NOT built — see Findings; `winner_history` accumulates forward-looking folds instead.
