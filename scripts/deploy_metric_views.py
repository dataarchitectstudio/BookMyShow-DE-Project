#!/usr/bin/env python3
"""Deploy governed metric view DDL (CREATE OR REPLACE VIEW ... WITH METRICS) to a
Unity Catalog gold schema.

Metric views have no native Databricks Asset Bundle resource type (confirmed via
`databricks bundle schema` as of CLI v1.9.0), so `databricks bundle deploy` never
touches them. This script is the CD-driven replacement for the manual
substitute-and-execute runbook step: it reads every `*.sql` file in the metric views
directory, substitutes `${catalog}`/`${gold_schema}`, and executes each as DDL via a
SQL warehouse. `CREATE OR REPLACE VIEW` is idempotent, so it's safe to run on every
deploy.

Auth is picked up by WorkspaceClient() from the standard Databricks env vars
(DATABRICKS_HOST/DATABRICKS_TOKEN) or a CLI profile (DATABRICKS_CONFIG_PROFILE).
"""

import argparse
import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

TERMINAL_STATES = {
    StatementState.SUCCEEDED,
    StatementState.FAILED,
    StatementState.CANCELED,
    StatementState.CLOSED,
}

DEFAULT_METRIC_VIEWS_DIR = (
    Path(__file__).resolve().parent.parent / "src" / "bookmyshow_pipeline" / "metric_views"
)


def deploy_view(w: WorkspaceClient, warehouse_id: str, name: str, statement: str) -> None:
    print(f"Deploying {name} ...")
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="30s",
    )
    while resp.status.state not in TERMINAL_STATES:
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)

    if resp.status.state != StatementState.SUCCEEDED:
        error = resp.status.error
        message = error.message if error else "unknown error"
        raise RuntimeError(f"{name} failed ({resp.status.state.value}): {message}")

    print(f"  -> {name} OK")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--gold-schema", required=True)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--metric-views-dir", type=Path, default=DEFAULT_METRIC_VIEWS_DIR)
    args = parser.parse_args()

    sql_files = sorted(args.metric_views_dir.glob("*.sql"))
    if not sql_files:
        print(f"No .sql files found under {args.metric_views_dir}", file=sys.stderr)
        sys.exit(1)

    w = WorkspaceClient()

    for path in sql_files:
        statement = path.read_text()
        statement = statement.replace("${catalog}", args.catalog).replace(
            "${gold_schema}", args.gold_schema
        )
        deploy_view(w, args.warehouse_id, path.name, statement)


if __name__ == "__main__":
    main()
