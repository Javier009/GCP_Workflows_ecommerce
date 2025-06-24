CREATE OR REPLACE TABLE `gcp-workflows-463821.ecommerce_data.ecommerce_staging_table` (
  user_id STRING,
  action STRING,
  timestamp TIMESTAMP,
  details  STRING
)
;

CREATE OR REPLACE TABLE `gcp-workflows-463821ecommerce_data.ecommerce_scd2_table` (
  user_id STRING,
  action STRING,
  timestamp TIMESTAMP,
  details  STRING,
  effective_start_date DATE,
  effective_end_date DATE,
  is_current BOOL
)

-- bq --project_id gcp-workflows --location US query --use_legacy_sql=false < /Users/delgadonoriega/Desktop/gcp-data-eng-bootcamp/Module_4_class_2/ecommerce-workflow/SCD2_process/tables_creation.sql