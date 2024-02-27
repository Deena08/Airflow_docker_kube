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