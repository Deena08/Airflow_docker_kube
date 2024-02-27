from configs.pipeline_config.global_config import *
from helper import *
from ml_demand_forecast_ import DaysOfCover
import ast

    ELIGIBLE_STORES = Variable.get("eligible_stores")
    ELIGIBLE_STORES = ast.literal_eval(ELIGIBLE_STORES)

    CHUNK_SIZE = Variable.get("batch_processing_chunk_size")
    CHUNK_SIZE = eval(CHUNK_SIZE)

    counter = 1

    for store_codes in batch(ELIGIBLE_STORES, CHUNK_SIZE):
        ml_demand_forecast_model = DaysOfCover(self,
           #task_id="graas_data_mart_ml_demand_forecast" + str(counter),
            store_codes
        )
        counter = counter + 1
