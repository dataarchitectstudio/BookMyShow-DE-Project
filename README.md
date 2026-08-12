# BookMyShow Data Platform

A Databricks Asset Bundle (DABs) implementing an end-to-end lakehouse for BookMyShow's
booking ecosystem: synthetic data generation, medallion (bronze/silver/gold) ingestion
and transformation via a Lakeflow Declarative Pipeline, governed KPIs as Unity Catalog
metric views, and an executive AI/BI dashboard — refreshed daily on an automated
schedule.

See [`Business-Problem & Architecture/`](Business-Problem%20&%20Architecture/) for the
full business problem statement and step-by-step implementation plan.

## Architecture

![BookMyShow Data Platform Architecture](Business-Problem%20&%20Architecture/bookmyshow-architecture-animated.svg)

**Flow:** synthetic booking-ecosystem data (theatres, movies, customers, shows,
bookings) lands in a Unity Catalog volume → **bronze** (Auto Loader, minimal
transformation) → **silver** (cleaned, deduplicated, data-quality expectations
enforced) → **gold** (dimensional facts/dims + daily/movie-performance aggregates) →
governed **metric views** (KPIs, defined once) → **AI/BI dashboard** for leadership.
The whole chain runs on a daily schedule via Lakeflow Jobs, with a separate on-demand
job for a full historical rebuild.

## The Business Problem

BookMyShow processes bookings across hundreds of theatres and thousands of shows daily,
but the underlying data has no governed analytics layer on top of it. Three problems
drove this build:

1. **No unified view of revenue and booking performance** — numbers live in
   disconnected sources and are reconciled manually.
2. **Payment failures go undetected until customers complain** — no systematic way to
   spot a gateway/city outage as it happens.
3. **No single source of truth for customer/theatre/movie performance** — every team
   calculates the same numbers differently.

Full details: [business goals](Business-Problem%20&%20Architecture/BookMyShow-Data-Platform-Business-goals.md).

## Project Structure

```
.
├── Business-Problem & Architecture/
├── databricks.yml
├── pyproject.toml
├── resources/
│   ├── bookmyshow_uc.yml
│   ├── bookmyshow_etl.pipeline.yml
│   ├── bookmyshow_daily_refresh.job.yml
│   ├── bookmyshow_full_rebuild.job.yml
│   └── bookmyshow_dashboard.yml
├── src/
│   ├── bookmyshow/
│   │   ├── generator.py
│   │   ├── schemas.py
│   │   └── main.py
│   └── bookmyshow_pipeline/
│       ├── transformations/
│       │   ├── bronze/
│       │   ├── silver/
│       │   └── gold/
│       ├── metric_views/
│       ├── dashboards/
│       └── explorations/
├── tests/
└── dist/
```

### Medallion layer tables

| Layer  | Tables |
|--------|--------|
| Bronze | `bronze_theatres`, `bronze_movies`, `bronze_customers`, `bronze_shows`, `bronze_bookings` |
| Silver | `silver_theatres`, `silver_movies`, `silver_customers`, `silver_shows`, `silver_bookings` |
| Gold   | Dims: `gold_dim_theatre`, `gold_dim_movie`, `gold_dim_customer`, `gold_dim_show`, `gold_dim_date` <br> Facts: `gold_fact_bookings`, `gold_fact_show_occupancy` <br> Aggregates: `gold_agg_daily_revenue_summary`, `gold_agg_movie_performance` |

## Jobs

| Job | Trigger | What it does |
|-----|---------|---------------|
| `bookmyshow_daily_refresh` | Periodic, every 1 day | Generates a day's worth of incremental synthetic data (new customers, shows, bookings), lands it in the `raw_landing` volume, then triggers an incremental refresh of the `bookmyshow_etl` pipeline. |
| `bookmyshow_full_rebuild` | On-demand only | Regenerates the complete synthetic dataset from scratch (~150K customers, ~120K shows, ~2M bookings) and runs a full-refresh of the pipeline. Kept separate from the daily job so a full rebuild never happens by accident. |

Both jobs run the `bookmyshow` Python wheel (`main.py`) as a `python_wheel_task`,
followed by a `pipeline_task` that runs `bookmyshow_etl`. Deploying with `mode:
production` (the `prod` target) activates the daily schedule; `mode: development`
(`dev` target, default) deploys everything paused.

## KPIs

KPIs are defined once as Unity Catalog **metric views** (`src/bookmyshow_pipeline/metric_views/`)
on top of the gold layer, so every consumer (dashboard, SQL, BI tool) gets the same
governed numbers instead of recalculating them independently.

| Metric View | Source | Key measures |
|-------------|--------|---------------|
| `mv_booking_revenue` | `gold_fact_bookings` | Total Revenue, Total/Successful/Failed Bookings, Average Ticket Price, Cancellation Rate, Unique Customers — sliced by date, city, movie, genre, theatre, booking channel, payment gateway |
| `mv_payment_health` | `gold_fact_bookings` | Total Bookings, Failed Bookings, **Payment Failure Rate** — sliced by payment gateway, city, and date, so a localized/gateway-specific spike is traceable immediately |
| `mv_theatre_occupancy` | `gold_fact_show_occupancy` | Show Count, Total Seat Capacity, Total Seats Sold, **Occupancy Rate** — sliced by theatre, city, screen type, and show date |

Additional KPIs (movie performance classification, revenue by city, top-performing
movies, customer loyalty segmentation) are computed in the gold aggregate tables
(`gold_agg_movie_performance`, `gold_agg_daily_revenue_summary`) that back these views
and the dashboard.

These feed the **BookMyShow Analytics** AI/BI dashboard
(`bookmyshow_analytics.lvdash.json`, deployed via `bookmyshow_dashboard.yml`), which has
three pages: Executive KPIs, Trends & Reach (payment-failure spike traceability by
gateway/city/date), and Movie Performance.

## Getting started

Choose how you want to work on this project:

(a) Directly in your Databricks workspace, see
    https://docs.databricks.com/dev-tools/bundles/workspace.

(b) Locally with an IDE like Cursor or VS Code, see
    https://docs.databricks.com/dev-tools/vscode-ext.html.

(c) With command line tools, see https://docs.databricks.com/dev-tools/cli/databricks-cli.html

If you're developing with an IDE, dependencies for this project should be installed using uv:

*  Make sure you have the UV package manager installed.
   It's an alternative to tools like pip: https://docs.astral.sh/uv/getting-started/installation/.
*  Run `uv sync --dev` to install the project's dependencies.


# Using this project using the CLI

The Databricks workspace and IDE extensions provide a graphical interface for working
with this project. It's also possible to interact with it directly using the CLI:

1. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

2. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project: the `bookmyshow_etl`
    pipeline, the `bookmyshow_daily_refresh` and `bookmyshow_full_rebuild` jobs, the
    `bookmyshow_analytics` dashboard, and the Unity Catalog bronze/silver/gold schemas
    and `raw_landing` volume.
    You can find these resources by opening your workspace and clicking on **Jobs & Pipelines**.

3. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```
   `bookmyshow_daily_refresh` runs the pipeline every day. The schedule is paused when
   deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

4. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```

5. Metric views (`src/bookmyshow_pipeline/metric_views/*.sql`) aren't yet a native DABs
   resource type — deploy them as DDL via the CLI once the gold schema exists (see the
   implementation plan's Governance & CI/CD section for the full runbook).

6. Finally, to run tests locally, use `pytest`:
   ```
   $ uv run pytest
   ```
