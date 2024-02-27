RAW_DATA_S3_BUCKET = 'gp-green-lake'
RAW_DATA_S3_BUCKET_STAGE = 'gp-green-lake-uat'
CONFIGURATION_S3_BUCKET = 'gp-configurations'
GRAAS_DATA_HUB_STORE_CODE = 'graas_data_hub'
GRAAS_DATA_MART_STORE_CODE = 'graas_data_mart'


# S3 Job codes
DATA_MART_INTEGRATION_CODE = "data_mart"

# AREA CODES
LOCAL_ENVIRONMENT_STORE_CODE = "dev"
STAGE_ENVIRONMENT_STORE_CODE = "stage"
UAT_ENVIRONMENT_STORE_CODE = "uat"
PROD_ENVIRONMENT_STORE_CODE = "prod"

# DBT profiles
DBT_ETL_SNOWFLAKE_PROFILE = "snowflake-etl-prd"
DBT_DATAMART_INSIGHTS_SNOWFLAKE_PROFILE = "snowflake-etl-prd"
DBT_DATAMART_DS_ML_PROFILE = "snowflake-ds-prd"
DBT_REPORTING_SNOWFLAKE_PROFILE = "snowflake-rpt-prd"
DBT_REDSHIFT_PROFILE = "rs_devtest"
DBT_ATTRIBUTION_SNOWFLAKE_PROFILE = "snowflake-attr-prd"
DBT_RULE_BASED_INSIGHTS_RECOS_SNOWFLAKE_PROFILE = "snowflake-etl-prd"
DBT_SQL_INSIGHTS_RECOS_SNOWFLAKE_PROFILE = "snowflake-insights-sql-based"
DBT_CUSTOM_INSIGHTS_RECOS_SNOWFLAKE_PROFILE = "snowflake-insights-sql-based"
DBT_PRIORITY_MERCHANTS_SNOWFLAKE_PROFILE = "snowflake-priority-prd"

# DynamoDB Tables
DYNAMO_DB_TABLE_STAGE = 'sia_store_code_lookup_stage'
DYNAMO_DB_TABLE_PROD = 'sia_store_code_lookup_prod'

#AWS
KINESIS_STREAM_PROD = 'graas_insight_reco_stream_prod'

KINESIS_STREAM_STAGING = 'graas_insight_reco_stream_stage'