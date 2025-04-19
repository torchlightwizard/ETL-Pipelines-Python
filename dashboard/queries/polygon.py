from src.extract.extract import FromDb
from schema.warehouse.polygon import schema_predict
from utils.ml.globals import DAYS_TO_LEARN
from datetime import datetime
import logging
import pandas as pd

def get(path="./data/datawarehouse/predict/polygon.csv"):
    df = pd.DataFrame([
            {"date": datetime(2000, 1, 1), "close": 0}, 
            {"date": datetime(2000, 1, 2), "close": 0}, 
            {"date": datetime(2000, 1, 3), "close": 0},
            {"date": datetime(2000, 1, 4), "close": 0},
            {"date": datetime(2000, 1, 5), "close": 0}
        ])
    try:
        df = FromDb("warehouse", "polygon_predict", schema_predict, "date", DAYS_TO_LEARN + 1).run().iloc[-5:]
    except Exception as err:
        logging.error(f"Function: dashboard.queries.polygon.get(). Failed to get prediction df. Error: {err}")
    return df

if __name__ == "__main__":
    print(get())