# BookMyShow Data Platform — Implementation Plan

## Context

BookMyShow's booking, show, movie, theatre, and customer data currently exists only as raw
operational data with no governed analytics layer (see
`Business-Problem & Architecture/BookMyShow-Data-Platform-Business-goals.md`). This causes
three costly problems: (1) revenue/booking performance is reconciled manually with no
unified trend view, (2) payment-gateway failures are discovered only after customer
complaints because there's no systematic way to spot a spike by gateway/city/date, and
(3) customer loyalty, theatre occupancy, and movie performance are calculated
inconsistently across teams. The ask is a governed Databricks data platform: reliable
ingestion, enforced data quality, a clean dimensional model, one governed definition per
KPI, and a leadership dashboard that makes a gateway/city/date payment-failure spike
obvious immediately — refreshed daily with no manual intervention and a controlled
dev→prod promotion path.

The repo started from a Databricks Asset Bundle (DABs) "default-python" template
(taxi sample code/pipeline/job), plus an already-agreed target architecture diagram
(`Business-Problem & Architecture/bookmyshow-architecture-animated.svg`) with five stages:
**Generate → Ingest → Refine (medallion, UC-governed) → Orchestrate → Analyze**. This plan
replaces the placeholder scaffolding with a concrete implementation of that architecture.
All bundle/resource/folder/package names below are the final, self-explanatory
BookMyShow-specific names actually used in the repo (bundle `bookmyshow_data_platform`,
package `bookmyshow`, pipeline source `bookmyshow_pipeline`, etc.) — no generic
placeholder template names remain anywhere in the project.

**Schema layout decision (confirmed with user):** medallion layers get **separate UC
schemas per layer** (bronze/silver/gold), not a single schema with prefixed table names —
this enables per-layer access control (e.g., BI users get `SELECT` on gold only) and is
the more idiomatic UC pattern, at the cost of a few new bundle variables.

**Status:** Naming foundation (bundle/package/resource names, UC schemas + volume), GENERATE,
INGEST (bronze), REFINE/Silver, REFINE/Gold, Governed KPIs (metric views), and the
Analyze dashboard are implemented, tested end-to-end on Databricks (`de-projects`
profile), and deployed to `dev`. Only step 9 (Governance & CI/CD hardening) remains.

---

## Naming convention

Catalog stays the existing `${var.catalog}` (`workspace` in both targets). Three bundle
variables (replacing the original template's single `schema` var) drive the medallion
schema layout:

| Variable | dev | prod |
|---|---|---|
| `bronze_schema` | `${workspace.current_user.short_name}_bronze` | `bronze` |
| `silver_schema` | `${workspace.current_user.short_name}_silver` | `silver` |
| `gold_schema` | `${workspace.current_user.short_name}_gold` | `gold` |

- Bronze tables: `<catalog>.<bronze_schema>.bronze_<entity>`
- Silver tables: `<catalog>.<silver_schema>.silver_<entity>`
- Gold tables: `<catalog>.<gold_schema>.gold_dim_<entity>` / `gold_fact_<entity>` / `gold_agg_<name>`
- Metric views (governed KPI layer, lives with gold): `<catalog>.<gold_schema>.mv_<domain>`
- Raw landing volume (pre-bronze staging): `<catalog>.<bronze_schema>.raw_landing` (UC managed volume)

**Object/folder naming (implemented):**

| Object | Name |
|---|---|
| Bundle (`databricks.yml` `bundle.name`) | `bookmyshow_data_platform` |
| Shared Python package | `bookmyshow` (folder `src/bookmyshow/`) |
| Pipeline source folder | `src/bookmyshow_pipeline/` (`transformations/`, `explorations/`) |
| Pipeline resource (key + name) | `bookmyshow_etl` (`resources/bookmyshow_etl.pipeline.yml`) |
| Daily refresh job (key + name) | `bookmyshow_daily_refresh` (`resources/bookmyshow_daily_refresh.job.yml`) |
| Daily job's generation task key | `generate_synthetic_data` |
| Daily job's pipeline-refresh task key | `refresh_pipeline` |
| UC foundation resource file | `resources/bookmyshow_uc.yml` (schemas `bronze_schema`/`silver_schema`/`gold_schema`, volume `raw_landing`) |

Note on DABs mechanics learned during implementation: in `mode: development`, DABs
auto-prefixes the *actual deployed name* of schema resources (e.g. `bronze_schema`'s
`name:` value becomes `dev_<user>_<value>` in the workspace) — so anywhere the real
runtime schema name is needed (pipeline `schema:`/`configuration`, job parameters), the
resource output `${resources.schemas.bronze_schema.name}` must be referenced instead of
the raw `${var.bronze_schema}`, otherwise dev and prod resolve inconsistently.

Lakeflow Declarative Pipelines support routing individual tables to a specific schema
within a single pipeline (multi-schema pipelines) rather than relying purely on one
pipeline-level default schema — each transformation file targets its own layer's schema
variable. **Verify exact current syntax against Lakeflow docs (`databricks-pipelines`
skill) at implementation time**, since this is a newer capability and syntax may have
shifted.

---

## 1. GENERATE — synthetic data ✅ implemented

**Builds:** `src/bookmyshow/generator.py` (Spark + Faker generators per entity),
`src/bookmyshow/schemas.py` (reference data, distributions, row-count constants),
`src/bookmyshow/main.py` (argparse entry point, replaces the original template's
placeholder taxi-sample logic).

**Capability:** `databricks-synthetic-data-gen` skill (Faker + Spark, serverless-friendly),
run via the `generate_synthetic_data` task (`python_wheel_task`, package `bookmyshow`,
entry point `main`) in the `bookmyshow_daily_refresh` job.

**Design:**
- `main.py` is `argparse`-driven: `--catalog --bronze-schema --mode {full,incremental}
  --run-date --seed --volume-path` plus per-entity row-count overrides.
- Row counts match the agreed architecture diagram: theatres 200, movies 600, customers
  150K, shows 120K, bookings 2.0M, seed 42.
- Two modes:
  - `full` — one-time bulk generation of the complete historical dataset, landed under
    `<entity>/full_load/`.
  - `incremental` — generates only "today's" new customers/shows/bookings under
    `<entity>/<run_date>/`, simulating new operational data landing daily (there's no real
    upstream system to ingest from). Reads back existing row counts to continue ID
    sequences and preserve referential integrity across runs.
- Output: Parquet, written to `/Volumes/<catalog>/<bronze_schema (resolved)>/raw_landing/<entity>/...`.
- Story baked into the data: a payment-gateway outage (`CCAvenue`, Mumbai + Pune,
  2026-07-30) spikes `payment_status='failed'` to ~88% for that gateway/city/date
  combination vs. a ~5% baseline elsewhere — this is what the Analyze step's dashboard
  must surface. Movie popularity is power-law distributed (real revenue concentration),
  and ~0.6% of bookings are deliberately corrupted (orphan customer FK, negative
  `ticket_amount`, null `booking_id`, duplicate rows) so the silver DQ layer (step 3) has
  real violations to catch.
- `pyproject.toml`: `faker` added to `dependencies`.
- `tests/test_generator.py`: unit tests for the pure (non-Spark) helpers — popularity
  weighting, threshold math, category/weight alignment (replaces the original template's
  placeholder test).

**Verified on Databricks:** deployed to `dev` (`de-projects` profile) and run via
`databricks bundle run bookmyshow_daily_refresh --only generate_synthetic_data`, in both
`full` and `incremental` modes — row counts, referential integrity (zero orphan show
references), the injected bad records, and the incident-date failure spike all confirmed
via SQL against the landed Parquet files.

---

## 2. INGEST — Auto Loader → Raw Landing Volume → Bronze *(planned)*

**Builds:**
- `resources/bookmyshow_uc.yml` *(already implemented — see Naming convention above)*.
- `src/bookmyshow_pipeline/transformations/bronze/bronze_theatres.py`,
  `bronze_movies.py`, `bronze_customers.py`, `bronze_shows.py`, `bronze_bookings.py`.

**Capability:** Auto Loader (`cloudFiles`) as DLT **streaming tables**
(`spark.readStream.format("cloudFiles")` under `@dp.table`), reading from the volume
paths above. Auto Loader's checkpoint + schema-tracking state gives exactly-once file
processing — this is the concrete mechanism satisfying "ingest without losing or
duplicating records."

**Design:** one bronze table per source entity (five total), append-only, minimal
transform (type casts + `_ingested_at`/`_source_file` lineage columns).
`cloudFiles.schemaEvolutionMode = addNewColumns`.

**Pipeline.yml change:** `resources/bookmyshow_etl.pipeline.yml` already has a
`configuration` block with `raw_volume_path` *(implemented)* so transformation files
don't hardcode paths; each bronze table decorator specifies `schema=<bronze_schema>`
explicitly (see multi-schema note above).

---

## 3. REFINE — Silver ✅ implemented

**Builds:** `src/bookmyshow_pipeline/transformations/silver/silver_theatres.py`,
`silver_movies.py`, `silver_customers.py`, `silver_shows.py`, `silver_bookings.py`.
`resources/bookmyshow_etl.pipeline.yml` gained a `silver_schema` configuration entry so
these files can target `${silver_schema}.<table>` explicitly (multi-schema pattern —
fully-qualified `name=` per table, since the pipeline's default schema stays bronze).

**Deviations from the original plan, confirmed reasonable during implementation:**
- `silver_bookings`/`silver_shows` referential integrity is enforced via a stream-static
  **inner join** against the cleaned dimension tables, not an `@dp.expect_or_drop`
  predicate — expectations are single-row SQL booleans and can't reference another
  table (no subqueries allowed).
- `silver_bookings` uses `@dp.expect_or_drop` (not `@dp.expect_or_fail`) on
  `booking_id`/`ticket_amount`. The GENERATE step deliberately injects a null
  `booking_id` / negative `ticket_amount` / orphan `customer_id` on **every** run
  (`schemas.BAD_RECORD_FRACTION`), specifically so silver has "real violations to catch
  and report" (see `schemas.py`) — `expect_or_fail` would hard-fail the pipeline on
  every single deploy, which contradicts that stated purpose.

**Verified on Databricks** (`de-projects` profile, `dev`): all 5 silver tables run via
`databricks bundle run bookmyshow_etl -t dev`. Confirmed via the DLT event log
(`event_log()`) and direct row-count comparisons: `silver_theatres`/`silver_movies`/
`silver_shows` pass through 1:1 (200/600/120,000 — no bad records injected for these
entities); `silver_customers` dedups to 150,000 via AUTO CDC with 0 expectation
failures; `silver_bookings` drops 5,279 negative-`ticket_amount` rows + 4,031
null-`booking_id` rows (`bookings_cleansed` expectations) plus orphan-FK/duplicate rows
via the join + AUTO CDC dedup, landing at 1,989,318 rows (down from 2,003,980 in
bronze). Caught and fixed one real bug along the way: the email-format `@dp.expect`
regex used single backslashes (`\s`, `\.`); Spark SQL string literals silently strip an
unrecognized single backslash before the regex engine sees it, corrupting the pattern
(measured 47% false-failure rate) — fixed by doubling the backslashes
(`\\s`/`\\.`) in the SQL string.

**Capability:** DLT/Lakeflow expectations (`@dp.expect`, `@dp.expect_or_drop`,
`@dp.expect_or_fail`) for declarative DQ, plus **AUTO CDC** (`create_auto_cdc_flow`) for
`silver_bookings` — bookings get status updates after generation time (e.g. a later
cancellation/refund), so keying on `booking_id` and sequencing by an event/ingestion
timestamp gives idempotent upsert semantics that solve both dedup and late-arriving
corrections in one mechanism, which is a better fit than plain `dropDuplicates`.

**Per-entity DQ rules (representative, not exhaustive):**
- `silver_theatres`: `expect_or_drop` non-null `theatre_id`/`city`/`state`; valid
  `screen_type` enum; `seat_capacity > 0`.
- `silver_movies`: non-null `movie_id`/`title`; `duration_minutes > 0`; non-null `genre`.
- `silver_customers`: non-null `customer_id`; `expect` (track, don't drop) on valid
  email/phone format; dedup by `customer_id` via AUTO CDC.
- `silver_shows`: non-null `show_id`; `base_price >= 0`; referential integrity to
  `silver_theatres`/`silver_movies` via `expect_or_drop`.
- `silver_bookings`: `expect_or_fail` (hard-stop) on non-null `booking_id` and
  `ticket_amount >= 0` (a violation here is a generation/modeling bug, not an edge case);
  `expect_or_drop` on `payment_status IN ('success','failed','refunded')` (matches the
  business problem statement's enum exactly — no `pending` state); referential integrity
  to `silver_shows`/`silver_customers`; uniqueness enforced structurally by AUTO CDC's
  keyed merge — this is exactly what the GENERATE step's injected orphan-FK/negative-
  amount/null-ID/duplicate records (see step 1) are designed to exercise.
- All silver tables' rejected-record counts are queryable via DLT's built-in event log
  (`event_log()` table function) — this is the observability proving DQ enforcement,
  without building custom tooling.

Each silver table decorator specifies `schema=<silver_schema>`.

---

## 4. REFINE — Gold (dimensional model) ✅ implemented

**Builds:** `src/bookmyshow_pipeline/transformations/gold/gold_dim_theatre.py`,
`gold_dim_movie.py`, `gold_dim_customer.py`, `gold_dim_show.py`, `gold_dim_date.py`,
`gold_fact_bookings.py`, `gold_fact_show_occupancy.py`, `gold_agg_movie_performance.py`,
`gold_agg_daily_revenue_summary.py`. `resources/bookmyshow_etl.pipeline.yml` gained a
`gold_schema` configuration entry (same multi-schema pattern as silver).

**Implementation choice:** every gold table is a `@dp.materialized_view()` (batch
`spark.read.table`), not a streaming table — including the fact tables. Gold reads from
`silver_bookings`, which is an AUTO CDC target that emits update/delete commits during
dedup; a streaming read of that would need `skipChangeCommits` and still miss
corrections. MVs recompute cleanly against current state instead, which the pipelines
skill's decision tree calls out as the preferred shape for "gold layer aggregation from
a streaming table" — full recompute is a non-issue at this data volume (~2M bookings).

**Business-rule choices made explicit in code (not fully specified in this plan):**
- `net_revenue = ticket_amount + fees - discount`, computed once in `gold_fact_bookings`.
- Only `payment_status = 'success' AND NOT is_cancelled` bookings count as "realized" —
  used consistently for `lifetime_spend`, occupancy `seats_sold`, and movie/daily revenue
  rollups, so a failed or cancelled booking never inflates a KPI.
- `loyalty_segment`: quartiles (`ntile(4)`) over `lifetime_spend`, richest first —
  Platinum/Gold/Silver/Bronze.
- `performance_tier`: revenue `percent_rank()` — top 20% "hit", next 40% "steady",
  bottom 40% "underperforming".
- `gold_dim_date`: generated (not sourced) via `sequence()`, covering 3 years back to 2
  years forward of `current_date()` — refreshed relative to run time, not hardcoded.

**Verified on Databricks** (`de-projects` profile, `dev`): all 9 gold tables built via
`databricks bundle run bookmyshow_etl -t dev`; row counts sane end to end
(`gold_fact_bookings` 1,989,318 = `silver_bookings` count; `gold_agg_daily_revenue_summary`
10,858 ≈ days × 15 cities × 4 gateways). Spot-checked KPI correctness directly via SQL:
loyalty quartiles show monotonically decreasing avg spend (₹17,751 → ₹1,353);
`performance_tier` shows the expected power-law separation (hit avg revenue ₹5.58M vs.
underperforming ₹38.7K); `gold_agg_daily_revenue_summary` reproduces the GENERATE step's
incident exactly — CCAvenue/Mumbai+Pune/2026-07-30 shows 88.5%/91.9% payment failure vs.
a 3-7% baseline for every other gateway/city that same day, i.e. the gold layer alone
already makes the incident "immediately obvious" in a raw query, before any dashboard
exists. Noted (not fixed, out of scope): 13 of 120,000 shows (0.01%) show
`occupancy_rate > 1` — the synthetic generator doesn't enforce a seat-inventory
constraint when creating bookings, so this is a GENERATE-step characteristic, not a gold
transformation defect.

**Star schema:**
- `gold_dim_theatre` — theatre_id, name, city, state, screen_type, seat_capacity (SCD1).
- `gold_dim_movie` — movie_id, title, genre, duration_minutes, release_date (SCD1).
- `gold_dim_customer` — profile attrs + **lifetime_spend** + **loyalty_segment**
  (Platinum/Gold/Silver/Bronze via percentile/threshold bucketing over aggregated spend
  from `gold_fact_bookings`) — the single place segmentation logic lives, directly
  answering business problem 3.
- `gold_dim_show` — show_id, theatre_id FK, movie_id FK, showtime, screen_type, base_price.
- `gold_dim_date` — standard calendar dimension, materialized once (not streaming).
- `gold_fact_bookings` — **grain: one row per booking.** Measures: seats_booked,
  ticket_amount, fees, discount, net_revenue (computed), payment_status, payment_gateway,
  booking_channel, is_cancelled. FKs: show_id, theatre_id, movie_id, customer_id, date_key.
  Primary fact behind revenue/booking-count/payment-failure/channel/cancellation KPIs.
- `gold_fact_show_occupancy` — **grain: one row per show** (kept separate from bookings to
  avoid double-counting seat capacity). Aggregates successful, non-cancelled seats booked
  per show against theatre/screen_type capacity → `occupancy_rate`.
- `gold_agg_movie_performance` — grain: one row per movie. Revenue rollup from
  `gold_fact_bookings`, bucketed into hit/steady/underperforming via percentile
  thresholds — computed once, not per-team.
- `gold_agg_daily_revenue_summary` — grain: date × city × gateway. This is the "Daily
  Batch Job → revenue summary" table from the architecture diagram; a direct rollup of
  `gold_fact_bookings`, so it can never disagree with the metric views.

All gold tables live in the same `bookmyshow_etl` pipeline as bronze/silver — no separate
deployable unit, just a `gold/` subfolder already covered by the existing `libraries.glob`
in `resources/bookmyshow_etl.pipeline.yml`.

---

## 5. Governed KPIs — Unity Catalog Metric Views ✅ implemented

The concrete mechanism for "one governed KPI definition, reused everywhere" — directly
targets business problems 2 and 3.

**Builds:** metric view YAML specs under `src/bookmyshow_pipeline/metric_views/`,
deployed via the `databricks-metric-views` skill into `<catalog>.<gold_schema>`.

**Proposed metric views:**
- `mv_booking_revenue` — source `gold_fact_bookings` + dims. Dimensions: date, city,
  movie, theatre, booking_channel, payment_gateway. Measures: total_revenue,
  total_bookings, successful_bookings, failed_bookings, avg_ticket_price,
  cancellation_rate, unique_customers. Answers: Total/Daily Revenue Trend,
  Total/Successful/Failed Bookings, Average Ticket Price, Cancellation Rate, Unique
  Customers, Revenue by City, booking channel split, Top movies by revenue.
- `mv_payment_health` — source `gold_fact_bookings`. Dimensions: payment_gateway, city
  (via theatre), booking_date. Measure: `payment_failure_rate`, plus raw failed/total
  counts. Slicing by all three dimensions simultaneously is the direct fix for problem 2
  (gateway/city/date spike traceability) — and is exactly what the GENERATE step's
  incident story (CCAvenue/Mumbai+Pune/2026-07-30) exists to prove out end to end.
- `mv_theatre_occupancy` — source `gold_fact_show_occupancy`. Dimensions: theatre, city,
  screen_type, date. Measure: occupancy_rate.
- Movie hit/steady/underperformer and customer loyalty segment are precomputed
  **classifications**, not aggregation-time measures — they stay as dimensions on
  `gold_agg_movie_performance`/`gold_dim_customer` rather than metric-view measures.
  Metric views own aggregation logic; gold tables own classification logic; both are
  governed because each lives in exactly one file.

**Resolved open item:** confirmed via `databricks bundle schema` (CLI v1.9.0) that DABs has
no native `metric_views` bundle resource type. Metric views are deployed as
`CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML` DDL, stored as
`src/bookmyshow_pipeline/metric_views/{mv_booking_revenue,mv_payment_health,
mv_theatre_occupancy}.sql` with `${catalog}`/`${gold_schema}` placeholders (substituted
per-target before running — these are plain SQL files, not bundle-templated resources).
**Runbook:** re-run the substitute-and-execute step whenever a `.sql` file changes;
`CREATE OR REPLACE VIEW` is idempotent DDL, so there's nothing to run on a schedule (no
job task needed, unlike the bronze/silver/gold pipeline).

**Verified on Databricks** (`de-projects` profile, `dev`): all 3 views created and queried
via `MEASURE()`. `mv_payment_health` reproduces the exact CCAvenue/Mumbai+Pune/2026-07-30
failure-rate numbers seen at the gold-table level (88.5%/91.9% vs. 3-7% baseline) —
confirming the governed layer doesn't drift from the underlying data. Cross-checked
`mv_booking_revenue`'s `Total Revenue` measure against a manual `SUM(net_revenue)` on
`gold_fact_bookings` (same WHERE clause): both return exactly 1,012,586,609, and
`mv_booking_revenue`'s top movie by revenue (₹8,746,272) matches
`gold_agg_movie_performance`'s `max_rev` for the `hit` tier — the two independently-built
layers agree.

---

## 6. ORCHESTRATE — Lakeflow Jobs

**Builds/changes:**
- `resources/bookmyshow_daily_refresh.job.yml` *(implemented)*: `generate_synthetic_data`
  (`python_wheel_task`, package `bookmyshow`, `--mode incremental`) →
  `refresh_pipeline` (`pipeline_task` against `bookmyshow_etl`, currently a no-op since
  bronze/silver/gold transformations don't exist yet — will do the real incremental
  bronze→silver→gold refresh including the revenue summary once steps 2–4 land). Daily
  `periodic` trigger. A leftover placeholder notebook task from the original template
  (dead code — it referenced a table that no longer exists) was removed entirely rather
  than renamed, since a renamed no-op still isn't "meaningful."
- **Still to build:** `resources/bookmyshow_full_rebuild.job.yml`: on-demand (no/paused
  trigger), `python_wheel_task` (`--mode full`) → `pipeline_task` with `full_refresh: true`
  at the task level. This is the explicit "full historical rebuild" capability the
  business problem requires, kept separate from the daily job so it never runs by
  accident.
- **Incremental vs. full rebuild mechanics:** bronze streaming tables + Auto Loader (once
  built) are incremental by construction (only new files since last checkpoint) —
  satisfies daily refresh automatically. `full_refresh: true` resets checkpoints/state and
  reprocesses everything — no separate pipeline definition needed, just a task-level flag
  on the same pipeline resource.
- `src/bookmyshow/` package and its wheel-task pattern are the permanent home for the
  generator; no further structural changes expected here.

---

## 7. ANALYZE — AI/BI Dashboard ✅ implemented

**Builds:** a Lakeview dashboard via the `databricks-aibi-dashboards` skill (all SQL
tested against deployed metric views/gold tables via CLI before deploying), checked in as
a bundle `dashboards` resource (`resources/bookmyshow_dashboard.yml`, referencing
`src/bookmyshow_pipeline/dashboards/bookmyshow_analytics.lvdash.json`).

**Implementation note — unlike metric views, dashboards ARE a native DABs bundle resource**
(confirmed via `databricks bundle schema`: `resources.dashboards.<key>` supports
`file_path`, `warehouse_id`, `dataset_catalog`, `dataset_schema`). This means the
dashboard JSON uses bare table/asset names throughout (`FROM mv_payment_health`,
`"asset_name": "mv_booking_revenue"`, no catalog/schema prefix), and
`dataset_catalog: ${var.catalog}` / `dataset_schema: ${resources.schemas.gold_schema.name}`
resolve it per-target on every `bundle deploy` — no manual per-environment step, unlike
the metric views in step 5.

**Pages built, exactly matching the plan below:**
- **Executive KPIs** — 7 counters (revenue, bookings, successful/failed, avg ticket
  price, cancellation rate, unique customers) + weekly revenue trend line, all from
  `mv_booking_revenue`.
- **Trends & Reach** — failure-rate KPI + a `Payment Failure Rate by Date and Gateway`
  line chart (the actual "immediately obvious" visual: CCAvenue's line spikes to
  ~89-92% for one day while the other three gateways stay flat near 5%) + a City x
  Gateway heatmap + a "worst combinations" table (`mv_payment_health`, `GROUP BY ALL
  HAVING total_bookings >= 20 ORDER BY failure_rate DESC LIMIT 15` — the incident lands
  in the top 2 rows with a 4x cliff down to the next-worst) + revenue-by-city and
  booking-channel-split from `mv_booking_revenue`.
- **Movie Performance** — top-10 movies bar chart (`mv_booking_revenue`, pre-limited via
  a small `GROUP BY ALL ORDER BY ... LIMIT 10` dataset, since widget-level queries can't
  express `LIMIT`), a detail table with hit/steady/underperforming coloring
  (`gold_agg_movie_performance`), and occupancy-by-screen-type / occupancy-by-city bar
  charts (`mv_theatre_occupancy`).
- **Global filters page**: Booking Date range + City + Payment Gateway multi-select,
  cascading to every dataset that shares that exact dimension name across the three
  governed metric views (a direct benefit of naming dimensions consistently in step 5).

**Deviation from the original page-4/9 plan sketch:** two dashboard datasets
(`ds_payment_worst`, `ds_top_movies`) use custom `queryLines` SQL against the metric
views (`SELECT ... MEASURE(...) FROM mv_x GROUP BY ALL HAVING/ORDER BY/LIMIT`) rather
than the `asset_name` shorthand, because Lakeview widget-level queries have no `LIMIT`/
`HAVING` construct — this was necessary to keep "top 10" / "worst 15" bounded. Both
still source exclusively from the governed metric views, preserving the single-
definition guarantee.

**Verified on Databricks** (`de-projects` profile, `dev`): `bundle deploy` created the
dashboard cleanly (no JSON parse errors), `databricks lakeview publish` succeeded, and
every widget's underlying query was independently re-run via SQL and confirmed to
return sane, non-empty results (revenue trend, failure-rate-by-date, the heatmap
grain, the worst-combinations table, top-10 movies, movie detail, occupancy by screen
type/city) — including reproducing the CCAvenue/Mumbai+Pune/2026-07-30 incident at the
top of the worst-combinations table exactly as designed.
Published URL:
`https://dbc-698fb84c-be59.cloud.databricks.com/dashboardsv3/01f1967ed3181f879a23b0aa8362aafd/published`

**Pages (mapped to the architecture diagram's three Analyze boxes):**
- **Executive KPIs** — total/daily revenue trend, total/successful/failed bookings, avg
  ticket price, cancellation rate, unique customers — from `mv_booking_revenue`.
- **Trends & Reach** — gateway × city × date payment-failure-rate heatmap/small-multiples
  from `mv_payment_health` (the page that must make a spike "immediately obvious" — a
  top-line failure-rate KPI card with red/amber/green thresholds plus the heatmap), plus
  revenue-by-city and booking-channel split from `mv_booking_revenue`.
- **Movie Performance** — top-10 movies by revenue (`mv_booking_revenue`) and a detail
  table with hit/steady/underperforming classification (`gold_agg_movie_performance`),
  plus theatre occupancy (`mv_theatre_occupancy`).

All charts query metric views (or gold aggregates that are direct rollups of them)
exclusively — never raw fact/dim tables — to enforce the single-definition guarantee end
to end.

**Deliberately out of scope for this plan:** proactive push alerting (e.g., a DBSQL Alert
on `mv_payment_health` that pages someone the moment a spike starts, rather than being
visible on next dashboard refresh). The agreed architecture diagram only shows a
dashboard, not an alerting component — flagged as a natural future enhancement, not core
MVP.

---

## Governance & CI/CD

- **Unity Catalog grants**: `resources/bookmyshow_uc.yml` *(schemas + volume implemented;
  grants still planned)* — `SELECT` on gold + metric views to a BI/dashboard-viewer group;
  tighter `SELECT`/`READ VOLUME` scoping on bronze/silver (raw + intermediate layers
  shouldn't be broadly readable). Exact grant statements via the `databricks-unity-catalog`
  skill.
- **Targets**: keep existing `dev`/`prod` DABs targets. `dev` (per-user isolated schemas,
  paused triggers) is where all authoring/iteration happens — safe to `bundle destroy`/
  redeploy repeatedly, giving the "confidently redeployed from a clean environment" story.
  `prod` (`mode: production`, fixed `bronze`/`silver`/`gold` schemas, explicit
  `root_path`, single-copy deploy) is the promotion target.
- **CLI profile (resolved):** `de-projects` — the only profile of the four
  (`DEFAULT`, `rag-project`, `dbc-698fb84c-be59`, `de-projects`) with a valid/active auth
  token against `dbc-698fb84c-be59.cloud.databricks.com`; confirmed with the user and used
  for all `dev` deploys/runs so far. Dev and prod currently share this one workspace (as
  configured in `databricks.yml`) — revisit only if workspace isolation becomes a
  requirement.
- **CI/CD**: no CI config exists today. Add a GitHub Actions workflow — on PR:
  `databricks bundle validate -t dev`, `ruff check`, `pytest` (against `fixtures/` sample
  data); on merge to main: `databricks bundle deploy -t prod`, gated by manual approval
  since `prod` uses `mode: production`.

---

## Phasing / dependency order

1. **Foundation** ✅ — `resources/bookmyshow_uc.yml` (3 schemas + volume), CLI profile
   confirmed (`de-projects`), `bundle validate -t dev` passing.
2. **Generate** ✅ — `src/bookmyshow` package (generator, schemas, main.py), unit tests,
   `pyproject.toml` `faker` dependency. Verified end-to-end on Databricks.
3. **Ingest (bronze)** ✅ — Auto Loader files in
   `src/bookmyshow_pipeline/transformations/bronze/`. Depends on (1)+(2).
4. **Refine (silver)** ✅ — DQ expectations + AUTO CDC in
   `src/bookmyshow_pipeline/transformations/silver/`. Depends on (3).
5. **Refine (gold)** ✅ — dims, facts, aggregates/classification in
   `src/bookmyshow_pipeline/transformations/gold/`. Depends on (4).
6. **Governed KPIs** ✅ — metric view specs atop gold. Depends on (5).
7. **Orchestrate** — daily job ✅ and full-rebuild job ✅ both scaffolded and refresh
   bronze→silver→gold end to end; metric views are deployed separately (DDL, not part
   of the pipeline/job resources — see step 6's runbook note).
8. **Analyze** ✅ — Lakeview dashboard against (6). Depends on (6).
9. **Governance & CI/CD hardening** *(next, and last remaining item)* — grants
   tightening, CI workflow, prod promotion dry-run.

Steps 3–5 are all authored inside the single existing `bookmyshow_etl` pipeline resource —
`resources/bookmyshow_etl.pipeline.yml` needs no further structural change (catalog,
schema, `raw_volume_path` configuration are already in place), just new files under
`transformations/{bronze,silver,gold}/` plus per-table `schema=` targeting.

---

## Verification

- `databricks bundle validate -t dev` after each resource-file change.
- `databricks bundle deploy -t dev` then `databricks bundle run bookmyshow_daily_refresh
  -t dev` (or the pipeline directly) to confirm bronze→silver→gold populates end to end
  with the injected bad records actually being caught (check DLT event log for expectation
  violation counts).
- Query each metric view directly (`execute_sql` / SQL warehouse) and cross-check a KPI
  (e.g., total revenue) computed two ways — via the metric view and via a manual
  `SUM(net_revenue)` on `gold_fact_bookings` — to confirm they agree.
- Open the deployed Lakeview dashboard and confirm the Trends & Reach page actually
  surfaces the synthetic gateway/city/date failure spike already baked into the GENERATE
  step's output (CCAvenue / Mumbai+Pune / 2026-07-30) — visually obvious, not buried in an
  overall average.
- `pytest` locally against `fixtures/` before every `bundle deploy -t prod`.
- Full pipeline run in `dev` with `full_refresh: true` to confirm the full-historical-
  rebuild path works before relying on it in prod.

### Critical files
- `databricks.yml` — bundle root (`bookmyshow_data_platform`), schema variables.
- `resources/bookmyshow_uc.yml` ✅ — schemas/volume/grants.
- `resources/bookmyshow_etl.pipeline.yml` ✅ — pipeline configuration (bronze/silver/gold
  all implemented).
- `resources/bookmyshow_daily_refresh.job.yml` ✅ — daily incremental refresh job.
- `resources/bookmyshow_full_rebuild.job.yml` ✅ — on-demand full rebuild job.
- `src/bookmyshow/main.py`, `generator.py`, `schemas.py` ✅ — synthetic data generation.
- `src/bookmyshow_pipeline/transformations/bronze/*.py` ✅,
  `src/bookmyshow_pipeline/transformations/silver/*.py` ✅,
  `src/bookmyshow_pipeline/transformations/gold/*.py` ✅.
- `src/bookmyshow_pipeline/metric_views/*.sql` ✅ (SQL DDL with embedded YAML, not
  standalone `.yml` — no native DABs bundle resource type for metric views; see step 5).
- `resources/bookmyshow_dashboard.yml` ✅ + `src/bookmyshow_pipeline/dashboards/
  bookmyshow_analytics.lvdash.json` ✅ — Analyze dashboard, IS a native bundle resource
  (unlike metric views); see step 7.
- `pyproject.toml` ✅ — `faker` dependency added.
