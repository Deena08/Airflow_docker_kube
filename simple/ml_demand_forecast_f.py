import numpy as np
import pandas as pd
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import *
from prophet import Prophet
from datetime import timedelta
from configs.pipeline_config.global_config import *
from sklearn.metrics import mean_absolute_percentage_error
from configs.ml_configs.ml_sql_configs import *
from dateutil.relativedelta import relativedelta
import ast, json
import time


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