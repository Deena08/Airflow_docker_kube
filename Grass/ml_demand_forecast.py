from airflow.models.connection import Connection
import json , ast ,time
import boto3
import os
import numpy as np
import pandas as pd
from io import StringIO
from airflow.hooks.base import BaseHook
from botocore.exceptions import ClientError
from airflow.models import Variable
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import *
from prophet import Prophet
from datetime import timedelta
from sklearn.metrics import mean_absolute_percentage_error
from dateutil.relativedelta import relativedelta
import requests

##Helper
def get_variables_file(integration_code):
    if os.environ['AIRFLOW_ENV'] == LOCAL_ENVIRONMENT_STORE_CODE:
        air_variables = Variable.get(integration_code, default_var='{}')
        return json.loads(air_variables)

    elif os.environ['AIRFLOW_ENV'] == STAGE_ENVIRONMENT_STORE_CODE or os.environ[
        'AIRFLOW_ENV'] == UAT_ENVIRONMENT_STORE_CODE or os.environ['AIRFLOW_ENV'] == PROD_ENVIRONMENT_STORE_CODE:
        s3 = boto3.client('s3')
        s3_file_key = os.environ['AIRFLOW_ENV'] + "/" + integration_code + ".json"
        try:
            result = s3.get_object(Bucket=CONFIGURATION_S3_BUCKET, Key=s3_file_key)
            air_variables = result["Body"].read().decode()
            if air_variables:
                return json.loads(air_variables)
            else:
                return dict()

        except ClientError as ex:
            print(ex)
            return dict()


    else:
        air_variables = Variable.get(integration_code, default_var='{}')
        return json.loads(air_variables)


"""
Write back integration based file path from S3 depending on the Environment
Dev setup can have Airflow variables created
Stage, UAT and Prod stores are pulled from S3 files
"""


def set_variables_file(data, integration_code):
    if os.environ['AIRFLOW_ENV'] == LOCAL_ENVIRONMENT_STORE_CODE:
        Variable.set(integration_code, data, serialize_json=True)

    elif os.environ['AIRFLOW_ENV'] == STAGE_ENVIRONMENT_STORE_CODE or os.environ[
        'AIRFLOW_ENV'] == UAT_ENVIRONMENT_STORE_CODE or os.environ['AIRFLOW_ENV'] == PROD_ENVIRONMENT_STORE_CODE:
        s3_vars = boto3.resource('s3')
        s3_file_key = os.environ['AIRFLOW_ENV'] + "/" + integration_code + ".json"
        s3_vars.Object(CONFIGURATION_S3_BUCKET, s3_file_key).put(Body=StringIO(json.dumps(data)).read())

    else:
        Variable.set(integration_code, data, serialize_json=True)


"""
Get S3 bucket name for data based on the environment
Stage, UAT and Prod files are pushed to different buckets
"""


def get_s3_bucket_name():
    if os.environ['AIRFLOW_ENV'] == PROD_ENVIRONMENT_STORE_CODE:
        return RAW_DATA_S3_BUCKET

    else:
        return RAW_DATA_S3_BUCKET_STAGE


"""
Get DynamoDB Table for data based on the environment
"""


def get_dynamodb_table():
    if os.environ['AIRFLOW_ENV'] == PROD_ENVIRONMENT_STORE_CODE:
        return DYNAMO_DB_TABLE_PROD

    else:
        return DYNAMO_DB_TABLE_STAGE


def batch(iterable, n=1):
    """Return batch wise data for given iterable

    Args:
        iterable (list)
        n (int, optional): Chunk size. Defaults to 1.

    Yields:
        list: chunked list
    """
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def format_organisation_data(organisation_data):
    org_data = dict()
    for org_data_dict in organisation_data:
        org_data["store_code"] = org_data_dict["quadrant"]
    return org_data


def get_kinesis_stream_name():
    if os.environ['AIRFLOW_ENV'] == PROD_ENVIRONMENT_STORE_CODE:
        return KINESIS_STREAM_PROD
    else:
        return KINESIS_STREAM_STAGING

    
####global_config
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

class DaysOfCover:
    def __init__(self, _store_code):
        #super().__init__(*args, **kwargs)
        self.store_code = _store_code
        self.connection = BaseHook.get_connection("snowflake_ds_connection")
        self.no_of_stores = 0
        self.store_channel_combinations = pd.DataFrame()
        self.all_combinations = pd.DataFrame()
        self.combinations_with_required_start_date = pd.DataFrame()
        self.daily_combinations_with_required_end_date = pd.DataFrame()
        self.weekly_combinations_with_required_end_date = pd.DataFrame()
        self.daily_required_combinations = pd.DataFrame()
        self.weekly_required_combinations = pd.DataFrame()
        self.daily_no_of_combinations_after_cov_filter = 0
        self.weekly_no_of_combinations_after_cov_filter = 0
        self.daily_no_of_combinations_after_zero_percent_filter = 0
        self.weekly_no_of_combinations_after_zero_percent_filter = 0
        self.daily_no_of_combinations_after_mape_filter = 0
        self.daily_weekly_common_combinations = 0        
        self.weekly_no_of_combinations_after_mape_filter = 0

    def establish_connection(self):
        snowflake_conn_prop = {
            "account": self.connection.host,
            "user": self.connection.login,
            "password": self.connection.password,
            "role": json.loads(self.connection.extra)["role"],
            "database": json.loads(self.connection.extra)["database"],
            "warehouse": json.loads(self.connection.extra)["warehouse"],
            "schema": "GRAAS_DATA_HUB",
        }
        session = Session.builder.configs(snowflake_conn_prop).create()
        session.sql("use role {}".format(snowflake_conn_prop["role"])).collect()
        session.sql("use database {}".format(snowflake_conn_prop["database"])).collect()
        session.sql("use schema {}".format("GRAAS_DATA_HUB")).collect()
        session.sql("use warehouse {}".format(snowflake_conn_prop["warehouse"]))
        print("Connection with Snowflake Done")
        print(
            session.sql(
                "select current_warehouse(), current_database(), current_schema()"
            ).collect()
        )
        return session

    def get_data(self, session, store):
        try:
            query_df = pd.DataFrame(
                session.sql(SALES_FORECAST_INPUT_QUERY.format(store=store)).collect()
            )
        except Exception as e:
            query_df = pd.DataFrame()
            print(" In Exception ", str(e))
        print("query_df", query_df)
        return query_df

    def null_count(self, x):
        return x.isnull().sum()

    def find_outliers(self, x):
        Q1,Q3 = x.quantile([0.25,0.75])
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        outliers = (x < lower_bound) | (x > upper_bound)
        return outliers.sum()

    def filtered_data_for_prediction(self, df, frequency):
        analysis_date = pd.to_datetime("today").replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        df.dropna(subset=["SELLER_ID", "CHANNEL", "SELLER_SKU"], inplace=True)
        df["ITEM_QUANTITY"] = df["ITEM_QUANTITY"].astype("int32")
        df["ORDER_CREATED_TS"] = pd.to_datetime(df["ORDER_CREATED_TS"])
        current_seller_combination = df[['SELLER_ID', 'CHANNEL', 'SELLER_SKU']].drop_duplicates()
        self.all_combinations = pd.concat([self.all_combinations,current_seller_combination])
        current_seller_channel_combination = df[['SELLER_ID', 'CHANNEL']].drop_duplicates()
        self.store_channel_combinations = pd.concat([self.store_channel_combinations,current_seller_channel_combination])
        df_start = df[
            df["ORDER_CREATED_TS"] <= (analysis_date - relativedelta(months=6))
        ]
        # filter sku's which has atleast 6 months of data
        current_seller_combination_with_required_start_date = df_start[['SELLER_ID', 'CHANNEL', 'SELLER_SKU']].drop_duplicates()
        self.combinations_with_required_start_date = pd.concat([self.combinations_with_required_start_date,current_seller_combination_with_required_start_date])
        if frequency == DAILY:
            df_end = df[
                (df["ORDER_CREATED_TS"] >= (analysis_date - timedelta(days=7)))
                & (df["ORDER_CREATED_TS"] <= (analysis_date - timedelta(days=1)))
            ]
            current_seller_combination_with_required_end_date = df_end[['SELLER_ID', 'CHANNEL', 'SELLER_SKU']].drop_duplicates()
            self.daily_combinations_with_required_end_date = pd.concat([self.daily_combinations_with_required_end_date,current_seller_combination_with_required_end_date])
            current_seller_required_combinations = current_seller_combination_with_required_start_date.merge(current_seller_combination_with_required_end_date,
                    how="inner",on=["SELLER_ID", "CHANNEL", "SELLER_SKU"],)
            self.daily_required_combinations = pd.concat([self.daily_required_combinations,current_seller_required_combinations])
        elif frequency == WEEKLY:
            df_end = df[
                (df["ORDER_CREATED_TS"] >= (analysis_date - timedelta(days=28)))
                & (df["ORDER_CREATED_TS"] <= (analysis_date - timedelta(days=1)))
            ]
            current_seller_combination_with_required_end_date = df_end[['SELLER_ID', 'CHANNEL', 'SELLER_SKU']].drop_duplicates()
            self.weekly_combinations_with_required_end_date = pd.concat([self.weekly_combinations_with_required_end_date,current_seller_combination_with_required_end_date])
            current_seller_required_combinations = current_seller_combination_with_required_start_date.merge(current_seller_combination_with_required_end_date,
                    how="inner",on=["SELLER_ID", "CHANNEL", "SELLER_SKU"],)
            self.weekly_required_combinations = pd.concat([self.weekly_required_combinations,current_seller_required_combinations])

        df = df.merge(
            current_seller_required_combinations,
            how="inner",
            on=["SELLER_ID", "CHANNEL", "SELLER_SKU"],
        )
        if frequency == WEEKLY:
            df = (
                df.groupby(
                    [
                        "SELLER_ID",
                        "CHANNEL",
                        "SELLER_SKU",
                        pd.Grouper(key="ORDER_CREATED_TS", freq="W-Sun"),
                    ]
                )
                .agg({"ITEM_QUANTITY": "sum"})
                .reset_index()
            )
        train_df = df[
            (df["ORDER_CREATED_TS"] >= (analysis_date - relativedelta(months=6)))
            & (df["ORDER_CREATED_TS"] <= (analysis_date - timedelta(days=1)))
        ]
        meta_data = (
            train_df.groupby(["SELLER_ID", "CHANNEL", "SELLER_SKU"])
            .agg(
                min_date=("ORDER_CREATED_TS", "min"),
                max_date=("ORDER_CREATED_TS", "max"),
                min=("ITEM_QUANTITY", "min"),
                max=("ITEM_QUANTITY", "max"),
                mean=("ITEM_QUANTITY", "mean"),
                median=("ITEM_QUANTITY", "median"),
                std=("ITEM_QUANTITY", "std"),
                count=("ITEM_QUANTITY", "count"),
                null_counts=pd.NamedAgg(
                    column="ITEM_QUANTITY", aggfunc=self.null_count
                ),
                # outlier_counts=pd.NamedAgg(
                #     column="ITEM_QUANTITY", aggfunc=self.find_outliers
                # ),
            )
            .reset_index()
        )

        meta_data["std"] = meta_data["std"].fillna(0)
        meta_data["cov"] = meta_data["std"] / meta_data["mean"]
        if frequency == DAILY:
            total_training_duration = (
                (analysis_date - timedelta(days=1))
                - (analysis_date - relativedelta(months=6))
            ) / np.timedelta64(1, "D")
        elif frequency == WEEKLY:
            total_training_duration = (
                (analysis_date - timedelta(days=1))
                - (analysis_date - relativedelta(months=6))
            ) / np.timedelta64(7, "D")
        meta_data["missing_dates_count"] = meta_data.apply(
            lambda row: total_training_duration - row["count"], axis=1
        )
        meta_data["zero_percentage"] = (
            meta_data["missing_dates_count"] / total_training_duration
        )
        filtered_meta_data = meta_data[(meta_data["cov"] <= 5)]
        if frequency == DAILY:
            self.daily_no_of_combinations_after_cov_filter += len(filtered_meta_data)
        elif frequency == WEEKLY:
            self.weekly_no_of_combinations_after_cov_filter += len(filtered_meta_data)
        filtered_meta_data = filtered_meta_data[(filtered_meta_data["zero_percentage"] <= 0.9)]
        if frequency == DAILY:
            self.daily_no_of_combinations_after_zero_percent_filter += len(filtered_meta_data)
        elif frequency == WEEKLY:
            self.weekly_no_of_combinations_after_zero_percent_filter += len(filtered_meta_data)
        filtered_train_df = pd.merge(
            train_df,
            filtered_meta_data[["SELLER_ID", "CHANNEL", "SELLER_SKU"]],
            on=["SELLER_ID", "CHANNEL", "SELLER_SKU"],
            how="right",
        )
        return filtered_train_df

    def prepare_data_frame_for_prediction(self, ts_group, frequency):
        analysis_date = pd.to_datetime("today").replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        time_series_df = ts_group.reset_index(drop=True).rename(
            columns={"ORDER_CREATED_TS": "ds", "ITEM_QUANTITY": "y"}
        )
        time_series_df["y"] = time_series_df["y"].astype(float)
        time_series_df["ds"] = pd.to_datetime(time_series_df["ds"])
        time_series_df["special_day"] = (
            time_series_df["ds"].dt.day == time_series_df["ds"].dt.month
        ).astype(int)
        time_series_df.sort_values(by="ds", inplace=True, ascending=True)
        time_series_df = time_series_df.set_index("ds")
        time_series_df = time_series_df.resample("D").mean()
        if frequency == DAILY:
            date_range = pd.date_range(
                (analysis_date - relativedelta(months=6)),
                (analysis_date - timedelta(days=1)),
                freq="D",
            )
        elif frequency == WEEKLY:
            date_range = pd.date_range(
                (analysis_date - relativedelta(months=6)),
                (analysis_date - timedelta(days=1)),
                freq="W-Sun",
            )
        time_series_df = time_series_df.reindex(date_range)
        time_series_df = time_series_df.fillna(0)
        time_series_df = time_series_df.reset_index()
        time_series_df.rename(columns={"index": "ds"}, inplace=True)
        time_series_df["day_of_week"] = time_series_df["ds"].apply(
            lambda date: date.weekday()
        )
        time_series_df["month"] = time_series_df["ds"].apply(lambda date: date.month)
        time_series_df["is_weekend"] = time_series_df["ds"].apply(
            lambda date: date.weekday() >= 5
        )
        return time_series_df

    def prophet_model_forecast(self, train_data, frequency):
        # print(train_data.head())
        groups = train_data.groupby(["SELLER_ID", "CHANNEL", "SELLER_SKU"])
        output_df_predict = pd.DataFrame(
            columns=[
                "SELLER_ID",
                "CHANNEL",
                "SELLER_SKU",
                "FORECAST_FREQUENCY",
                "FORECAST",
                "AVERAGE_SALES",
                "MAPE",
            ]
        )
        for (SELLER_ID, CHANNEL, SELLER_SKU), group in groups:
            print(SELLER_ID, CHANNEL, SELLER_SKU)
            ts_group = group[["ORDER_CREATED_TS", "ITEM_QUANTITY"]]
            train_ts_df = self.prepare_data_frame_for_prediction(ts_group, frequency)

            max = train_ts_df["y"].max()
            train_ts_df["floor"] = 0
            train_ts_df["cap"] = 0.25 * max
            x_train_ts_df = train_ts_df.drop("y", axis=1)

            model = Prophet(
                growth="logistic", weekly_seasonality=True, interval_width=0.95
            )
            if frequency == DAILY:
                model.add_regressor("special_day")
            model.add_regressor("day_of_week", mode="additive")
            model.add_regressor("month", mode="additive")
            model.add_regressor("is_weekend", mode="additive")
            model.fit(train_ts_df)
            forecast = model.predict(x_train_ts_df)
            forecast["y_actual"] = train_ts_df["y"]

            mape_df = forecast[forecast["y_actual"] != 0]
            mape_int = mean_absolute_percentage_error(
                mape_df["y_actual"], mape_df["yhat"].round().astype(int)
            )

            if frequency == DAILY:
                print(
                    f"Forecasting Daily Sales of {SELLER_ID, CHANNEL, SELLER_SKU} for 32 days"
                )
                # Prediction
                future = model.make_future_dataframe(
                    periods=32, freq="D"
                )  # Forecast for 32 days
                future["special_day"] = (
                    future["ds"].dt.day == future["ds"].dt.month
                ).astype(int)
                future["floor"] = 0
                future["cap"] = 0.25 * max

                # Add regressor values to the future DataFrame
                future["day_of_week"] = future["ds"].dt.dayofweek
                future["month"] = future["ds"].dt.month
                future["is_weekend"] = (future["ds"].dt.dayofweek >= 5).astype(int)
                forecast_future = model.predict(future)

                # Round 'y_hat' values and replace negative values with zero
                forecast_future["y_hat_rounded"] = (
                    forecast_future["yhat"].round().clip(lower=0)
                )
                average_sales = forecast_future["y_hat_rounded"].tail(32).mean()
                forecast_dict = dict(
                    zip(
                        forecast_future["ds"].tail(32).astype("str"),
                        forecast_future["y_hat_rounded"].tail(32),
                    )
                )

                dataset_results = pd.DataFrame(
                    {
                        "SELLER_ID": SELLER_ID,
                        "CHANNEL": CHANNEL,
                        "SELLER_SKU": SELLER_SKU,
                        "FORECAST_FREQUENCY": frequency,
                        "FORECAST": [forecast_dict],
                        "AVERAGE_SALES": average_sales,
                        "MAPE": mape_int,
                    }
                )
                output_df_predict = pd.concat(
                    [output_df_predict, dataset_results], ignore_index=True
                )
            elif frequency == WEEKLY:
                print(
                    f"Forecasting Weekly Sales of {SELLER_ID, CHANNEL, SELLER_SKU} for 4 weeks"
                )
                future = model.make_future_dataframe(
                    periods=4, freq="W-Sun"
                )  # Forecast for 4 weeks
                future["floor"] = 0
                future["cap"] = 0.25 * max
                # Add regressor values to the future DataFrame
                future["day_of_week"] = future["ds"].dt.dayofweek
                future["month"] = future["ds"].dt.month
                future["is_weekend"] = (future["ds"].dt.dayofweek >= 5).astype(int)
                forecast_future = model.predict(future)

                # Round 'y_hat' values and replace negative values with zero
                forecast_future["y_hat_rounded"] = (
                    forecast_future["yhat"].round().clip(lower=0)
                )
                average_sales = forecast_future["y_hat_rounded"].tail(4).mean()
                forecast_dict = dict(
                    zip(
                        forecast_future["ds"].tail(4).astype("str"),
                        forecast_future["y_hat_rounded"].tail(4),
                    )
                )

                dataset_results = pd.DataFrame(
                    {
                        "SELLER_ID": SELLER_ID,
                        "CHANNEL": CHANNEL,
                        "SELLER_SKU": SELLER_SKU,
                        "FORECAST_FREQUENCY": frequency,
                        "FORECAST": [forecast_dict],
                        "AVERAGE_SALES": average_sales,
                        "MAPE": mape_int,
                    }
                )
                output_df_predict = pd.concat(
                    [output_df_predict, dataset_results], ignore_index=True
                )
        return output_df_predict

    # noinspection PyUnreachableCode
    def execute(self, context):
        schema = StructType(
            [
                StructField("SELLER_ID", StringType()),
                StructField("CHANNEL", StringType()),
                StructField("SELLER_SKU", StringType()),
                StructField("FORECAST_FREQUENCY", StringType()),
                StructField("FORECAST", VariantType()),
                StructField("AVERAGE_SALES", DecimalType(38, 4)),
                StructField("MAPE", DecimalType(38, 4)),
                StructField("FORECAST_RUN_DATE", TimestampType()),
            ]
        )
        session = self.establish_connection()
        # store_codes = pd.DataFrame(session.sql(STORE_CODES_QUERY).collect())['SELLER_ID'].tolist()
        # all_combinations_daily = pd.DataFrame()
        for store in self.store_code:
            self.no_of_stores += 1
            final_df = pd.DataFrame()
            start_time = time.time()
            try:
                print(f"Fetching {store} data")
                data = self.get_data(session, store)
                # print(f"{store} Data Fetched")
                # print(data.head())
                print(f"Fetching {store} daily training data")
                train_data_daily = self.filtered_data_for_prediction(data, DAILY)
                # print(f"{store} Daily Training Data Fetched")
                result_df_daily = self.prophet_model_forecast(train_data_daily, DAILY)
                print(f"Daily Model Built for {store}")
                result_df_daily_mape_filter = result_df_daily[
                    result_df_daily["MAPE"] <= 1
                ]
                self.daily_no_of_combinations_after_mape_filter += len(result_df_daily_mape_filter)                
                current_seller_combinations_daily = result_df_daily_mape_filter[
                    ["SELLER_ID", "CHANNEL", "SELLER_SKU"]
                ].drop_duplicates()
                # all_combinations_daily = pd.concat([all_combinations,current_seller_combinations_daily])
                final_df = pd.concat([final_df, result_df_daily_mape_filter])
                print(f"Fetching {store} weekly training data")
                train_data_weekly = self.filtered_data_for_prediction(data, WEEKLY)
                # print(f"{store} Weekly Training Data Fetched")
                intersection_df = pd.merge(current_seller_combinations_daily,train_data_weekly[["SELLER_ID", "CHANNEL", "SELLER_SKU"]],
                                           on=["SELLER_ID", "CHANNEL", "SELLER_SKU"],how="inner")
                self.daily_weekly_common_combinations += len(intersection_df.drop_duplicates())

                merged = pd.merge(
                    train_data_weekly,
                    current_seller_combinations_daily,
                    on=["SELLER_ID", "CHANNEL", "SELLER_SKU"],
                    how="outer",
                    indicator=True,
                )
                train_data_weekly_only = merged[merged["_merge"] == "left_only"].drop(
                    "_merge", axis=1
                )
                result_df_weekly = self.prophet_model_forecast(
                    train_data_weekly_only, WEEKLY
                )
                print(f"Weekly Model Built for {store}")
                result_df_weekly_mape_filter = result_df_weekly[
                    result_df_weekly["MAPE"] <= 1
                ]
                self.weekly_no_of_combinations_after_mape_filter += len(result_df_weekly_mape_filter)
                final_df = pd.concat([final_df, result_df_weekly_mape_filter])
                final_df["FORECAST_RUN_DATE"] = pd.Timestamp.now()

                result_df = session.create_dataframe(
                    final_df.values.tolist(), schema=schema
                )
                # print(pd.DataFrame(result_df.collect()).head())
                result_df.write.mode("append").saveAsTable(
                    "graas_data_mart" + ".ml_demand_forecast"
                )
                print(f"Rows inserted successfully for {store}")
            except Exception as e:
                print(" In Exception ", str(e))
                result_df = pd.DataFrame(
                    columns=[
                        "SELLER_ID",
                        "CHANNEL",
                        "SELLER_SKU",
                        "FORECAST_FREQUENCY",
                        "FORECAST",
                        "AVERAGE_SALES",
                        "MAPE",
                        "FORECAST_RUN_DATE",
                    ]
                )
                result_df = session.create_dataframe(
                    result_df.values.tolist(), schema=schema
                )
                result_df.write.mode("append").saveAsTable(
                    "graas_data_mart" + ".ml_demand_forecast"
                )
            end_time = time.time()
            print(f"Time taken for {store}", (end_time - start_time))
        print('Total stores: ',self.no_of_stores)
        print('Total seller + channel combinations: ',len(self.store_channel_combinations.drop_duplicates()))
        print('Total SKU combinations: ',len(self.all_combinations.drop_duplicates()))
        print('Total SKU combinations with required start date: ',len(self.combinations_with_required_start_date.drop_duplicates()))

        print('Total SKU daily combinations with required end date: ',len(self.daily_combinations_with_required_end_date.drop_duplicates()))
        print('Total SKU daily required combinations: ',len(self.daily_required_combinations.drop_duplicates()))
        print('Total SKU daily no of combinations after cov filter: ',self.daily_no_of_combinations_after_cov_filter)
        print('Total SKU daily no of combinations after zero percentage filter: ',self.daily_no_of_combinations_after_zero_percent_filter)
        print('Total SKU daily no of combinations after mape filter: ',self.daily_no_of_combinations_after_mape_filter)      

        print('Total SKU weekly combinations with required end date: ',len(self.weekly_combinations_with_required_end_date.drop_duplicates()))
        print('Total SKU weekly required combinations: ',len(self.weekly_required_combinations.drop_duplicates()))
        print('Total SKU weekly no of combinations after cov filter: ',self.weekly_no_of_combinations_after_cov_filter)
        print('Total SKU weekly no of combinations after zero percentage filter: ',self.weekly_no_of_combinations_after_zero_percent_filter)
        print('Total daily weekly common SKU combinations: ',self.daily_weekly_common_combinations)
        print('Total SKU weekly no of combinations after mape filter: ',self.weekly_no_of_combinations_after_mape_filter)        
        
        return "Success"
    
####ml_sql_configs

INPUT_DATA_QUERY = "select date(report_date) as date , a.sub_channel ,a.metric_name as metric ,a.dimension_1_name as dimension ,a.dimension_1_value DIMENSION_VALUE ,coalesce(NULLIF(a.dimension_1_id, ''),a.dimension_1_value) as dimension_id , sum(a.current_day_value) metric_value from {store_code}.fact_insights_day_aggr_history a join ( select  sub_channel , metric_name , dimension_1_name , dimension_1_value ,coalesce(NULLIF(dimension_1_id, ''),dimension_1_value) as dimension_1_id from ( select sub_channel , metric_name , dimension_1_name , dimension_1_value , coalesce(NULLIF(dimension_1_id, ''),dimension_1_value) as dimension_1_id , sum(current_day_value) as metric_value , row_number() OVER ( PARTITION BY sub_channel,metric_name,dimension_1_name  ORDER BY metric_value DESC) AS row_num from {store_code}.fact_insights_day_aggr_history fidah join global.config_ml_metric_dimensions c on fidah.sor = c.sor and fidah.metric_name = c.metric and fidah.dimension_1_name = c.dimension where report_date between current_date -  {top_n_start_date} and current_date -1 and NOT (lower(dimension_1_value)  LIKE ANY ('%blank%', '%(not_set)%')) group by 1,2,3,4,5 ) where row_num <= {top_n} ) b on a.sub_channel = b.sub_channel and a.metric_name = b.metric_name and a.dimension_1_name = b.dimension_1_name and a.dimension_1_value = b.dimension_1_value and dimension_id = b.dimension_1_id where a.report_date between current_date - {traning_start_date} and current_date -1 group by 1,2,3,4,5,6"


TOP_N = "select top_n from  global.config_ml_store_parameters where lower(store_code) = lower('{store_code}')"


TRAINING_DAYS = "select start_date_model_training  from  global.config_ml_store_parameters where lower(store_code) = lower('{store_code}')"


TOP_N_DAYS = "select start_date_top_performing from  global.config_ml_store_parameters where lower(store_code) = lower('{store_code}')"

INPUT_DATA_QUERY = "select date(report_date) as date , a.sub_channel ,a.metric_name as metric ,a.dimension_1_name as dimension ,a.dimension_1_value DIMENSION_VALUE ,coalesce(NULLIF(a.dimension_1_id, ''),a.dimension_1_value) as dimension_id , sum(a.current_day_value) metric_value from {store_code}.fact_insights_day_aggr_history a join ( select  sub_channel , metric_name , dimension_1_name , dimension_1_value ,coalesce(NULLIF(dimension_1_id, ''),dimension_1_value) as dimension_1_id from ( select sub_channel , metric_name , dimension_1_name , dimension_1_value , coalesce(NULLIF(dimension_1_id, ''),dimension_1_value) as dimension_1_id , sum(current_day_value) as metric_value , row_number() OVER ( PARTITION BY sub_channel,metric_name,dimension_1_name  ORDER BY metric_value DESC) AS row_num from {store_code}.fact_insights_day_aggr_history fidah join global.config_ml_metric_dimensions c on fidah.sor = c.sor and fidah.metric_name = c.metric and fidah.dimension_1_name = c.dimension where report_date between current_date -  {top_n_start_date} and current_date -1 and NOT (lower(dimension_1_value)  LIKE ANY ('%blank%', '%(not_set)%')) group by 1,2,3,4,5 ) where row_num <= {top_n} ) b on a.sub_channel = b.sub_channel and a.metric_name = b.metric_name and a.dimension_1_name = b.dimension_1_name and a.dimension_1_value = b.dimension_1_value and dimension_id = b.dimension_1_id where a.report_date between current_date - {traning_start_date} and current_date -1 group by 1,2,3,4,5,6"


TOP_N = "select top_n from  global.config_ml_store_parameters where lower(store_code) = lower('{store_code}')"


TRAINING_DAYS = "select start_date_model_training  from  global.config_ml_store_parameters where lower(store_code) = lower('{store_code}')"


TOP_N_DAYS = "select start_date_top_performing from  global.config_ml_store_parameters where lower(store_code) = lower('{store_code}')"


COMBINED_QUERY_2X2 = """
with md as
(
    select
    dmm.metric_name,
    dmm.display_name as metric_display_name,
    dmd.dimension_name,
    dmd.display_name as dimension_display_name,
    concat(dms.sor_short_name, '__revenue__sum') as impact_metric
    from global.dim_meta_metric_dimension as dmmd
    left join global.dim_meta_metric as dmm on dmmd.metric_id = dmm.metric_id
    left join global.dim_meta_dimension as dmd on dmmd.dimension_id = dmd.dimension_id
    left join global.dim_meta_sor as dms on dms.sor_id = dmmd.sor_id
),
metric2_data as
(
    select 
        fiwa.SUB_CHANNEL 
        , fiwa.dimension_1_id 
        , fiwa.dimension_1_value
        , fiwa.dimension_1_name
        , fiwa.metric_name
        , fiwa.current_week_value as metric_2_value
        , md.metric_display_name as metric_2_display_name
    from {store_code}.fact_insights_week_aggr fiwa
    join md 
    on md.metric_name = fiwa.metric_name and fiwa.dimension_1_name = md.dimension_name
    where fiwa.metric_name = '{metric_2_name}' 
    and fiwa.dimension_1_name='{dimension_name}'
)
select 
      metric1_data.store_code as store_code
    , metric1_data.SUB_CHANNEL as sub_channel
    , metric1_data.dimension_1_id as dimension_id
    , metric1_data.dimension_1_value as dimension_value
    , metric1_data.dimension_1_name as dimension_name
    , metric1_data.metric_name as metric_1_name
    , metric2_data.metric_name as metric_2_name
    , metric1_data.current_week_value as metric_1_value
    , metric2_data.metric_2_value 
    , md.metric_display_name as metric_1_display_name
    , metric2_data.metric_2_display_name as metric_2_display_name
    , md.dimension_display_name as dimension_display_name
    , md.impact_metric as impact_metric
from {store_code}.fact_insights_week_aggr metric1_data
join metric2_data
on metric2_data.sub_channel = metric1_data.sub_channel
and metric2_data.dimension_1_id = metric1_data.dimension_1_id
and metric2_data.dimension_1_value = metric2_data.dimension_1_value
join md 
on md.metric_name = metric1_data.metric_name and metric1_data.dimension_1_name = md.dimension_name
where metric1_data.metric_name = '{metric_1_name}' 
and metric1_data.dimension_1_name='{dimension_name}'
order by metric1_data.sub_channel"""


METRIC_PAIR_COMBINATION = "select * from global.config_ml_metric_pair_dimensions;"

SALES_FORECAST_INPUT_QUERY = """select 
    date(convert_timezone('UTC', coalesce(am.timezone, 'Asia/Kolkata'), ofoi.order_created_ts)) order_created_ts,
    ofoi.seller_id,
    ofoi.channel,
    ofoi.seller_sku,
    sum(ofoi.item_quantity) as item_quantity
from graas_data_hub.ord_fct_order_items ofoi 
join global.merchant_channel_details am
on lower(am.store_code) = lower(ofoi.seller_id)
and lower(am.channel) = lower(ofoi.channel)
where 
ofoi.seller_id = '{store}'
and date(convert_timezone('UTC', coalesce(am.timezone, 'Asia/Kolkata'), ofoi.order_created_ts)) < current_date
group by 1,2,3,4;"""


DAILY = "D"
WEEKLY = "W"
COMBINED_QUERY_2X2 = """
with md as
(
    select
    dmm.metric_name,
    dmm.display_name as metric_display_name,
    dmd.dimension_name,
    dmd.display_name as dimension_display_name,
    concat(dms.sor_short_name, '__revenue__sum') as impact_metric
    from global.dim_meta_metric_dimension as dmmd
    left join global.dim_meta_metric as dmm on dmmd.metric_id = dmm.metric_id
    left join global.dim_meta_dimension as dmd on dmmd.dimension_id = dmd.dimension_id
    left join global.dim_meta_sor as dms on dms.sor_id = dmmd.sor_id
),
metric2_data as
(
    select 
        fiwa.SUB_CHANNEL 
        , fiwa.dimension_1_id 
        , fiwa.dimension_1_value
        , fiwa.dimension_1_name
        , fiwa.metric_name
        , fiwa.current_week_value as metric_2_value
        , md.metric_display_name as metric_2_display_name
    from {store_code}.fact_insights_week_aggr fiwa
    join md 
    on md.metric_name = fiwa.metric_name and fiwa.dimension_1_name = md.dimension_name
    where fiwa.metric_name = '{metric_2_name}' 
    and fiwa.dimension_1_name='{dimension_name}'
)
select 
      metric1_data.store_code as store_code
    , metric1_data.SUB_CHANNEL as sub_channel
    , metric1_data.dimension_1_id as dimension_id
    , metric1_data.dimension_1_value as dimension_value
    , metric1_data.dimension_1_name as dimension_name
    , metric1_data.metric_name as metric_1_name
    , metric2_data.metric_name as metric_2_name
    , metric1_data.current_week_value as metric_1_value
    , metric2_data.metric_2_value 
    , md.metric_display_name as metric_1_display_name
    , metric2_data.metric_2_display_name as metric_2_display_name
    , md.dimension_display_name as dimension_display_name
    , md.impact_metric as impact_metric
from {store_code}.fact_insights_week_aggr metric1_data
join metric2_data
on metric2_data.sub_channel = metric1_data.sub_channel
and metric2_data.dimension_1_id = metric1_data.dimension_1_id
and metric2_data.dimension_1_value = metric2_data.dimension_1_value
join md 
on md.metric_name = metric1_data.metric_name and metric1_data.dimension_1_name = md.dimension_name
where metric1_data.metric_name = '{metric_1_name}' 
and metric1_data.dimension_1_name='{dimension_name}'
order by metric1_data.sub_channel"""


METRIC_PAIR_COMBINATION = "select * from global.config_ml_metric_pair_dimensions;"

SALES_FORECAST_INPUT_QUERY = """select 
    date(convert_timezone('UTC', coalesce(am.timezone, 'Asia/Kolkata'), ofoi.order_created_ts)) order_created_ts,
    ofoi.seller_id,
    ofoi.channel,
    ofoi.seller_sku,
    sum(ofoi.item_quantity) as item_quantity
from graas_data_hub.ord_fct_order_items ofoi 
join global.merchant_channel_details am
on lower(am.store_code) = lower(ofoi.seller_id)
and lower(am.channel) = lower(ofoi.channel)
where 
ofoi.seller_id = '{store}'
and date(convert_timezone('UTC', coalesce(am.timezone, 'Asia/Kolkata'), ofoi.order_created_ts)) < current_date
group by 1,2,3,4;"""


DAILY = "D"
WEEKLY = "W"

def get_variable_data():
    
    global ELIGIBLE_STORES
    global CHUNK_SIZE
    
    ELIGIBLE_STORES = airflow_variable_get("eligible_stores")
    ELIGIBLE_STORES = ast.literal_eval(ELIGIBLE_STORES)
    CHUNK_SIZE = airflow_variable_get("batch_processing_chunk_size")
    CHUNK_SIZE = eval(CHUNK_SIZE)


def airflow_variable_get(key):
 try:
      url = f"http://localhost:8080/api/v1/variables/{key}"
      response = requests.get(url, auth=('airflow', 'airflow'))
        
      if response.status_code == 200:
            variable_value = response.json().get('value')
            return variable_value
      else:
            print("Failed to fetch variable. Status code:", response.status_code)
            return None
 except Exception as e:
        print("An error occurred:", str(e))
        return None


def main():

    get_variable_data()

    counter = 1

    for store_codes in batch(ELIGIBLE_STORES, CHUNK_SIZE):
     ml_demand_forecast_model = DaysOfCover(store_codes)
     counter = counter + 1
   
main()