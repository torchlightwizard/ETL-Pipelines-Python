from src.extract.extract import FromDb
from schema.warehouse.open_meteo import predict_schema
from datetime import datetime
import logging
import pandas as pd

def get(path="./data/datawarehouse/predict/openmeteo.csv"):
    df = pd.DataFrame([{"date": datetime(2000, 1, 1), "rain": 0}])
    try:
        df = FromDb("warehouse", "open_meteo_predict", predict_schema, "date", 1).run().iloc[[-1]]
    except Exception as err:
        logging.error(f"Function: dashboard.queries.open_meteo.get(). Failed to get prediction df. Error: {err}")
    return df


if __name__ == "__main__":
    print(get())