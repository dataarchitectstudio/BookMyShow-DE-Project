# bookmyshow_pipeline

This folder defines all source code for the `bookmyshow_etl` Lakeflow Declarative
Pipeline:

- `explorations/`: Ad-hoc notebooks used to explore the data processed by this pipeline.
- `transformations/`: All dataset definitions and transformations, organized into
  `bronze/`, `silver/`, and `gold/` subfolders matching the medallion layers.
- `utilities/` (optional): Utility functions and Python modules used in this pipeline.
- `data_sources/` (optional): View definitions describing the source data for this pipeline.

## Getting Started

To get started, go to the `transformations` folder -- most of the relevant source code lives there:

* By convention, every dataset under `transformations` is in a separate file.
* If you're using the workspace UI, use `Run file` to run and preview a single transformation.
* If you're using the CLI, use `databricks bundle run bookmyshow_etl --refresh <table_name>`
  to run a single transformation.

For more tutorials and reference material, see https://docs.databricks.com/dlt.
