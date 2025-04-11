from src.transform.transform import Transform
from src.extract.extract import FromDb
from utils.ml.globals import DAYS_TO_LEARN
from schema.warehouse.polygon import schema_data as wpdschema

import os
from datetime import timedelta

import numpy as np
import pandas as pd
import tensorflow as tf



class PJsonToDF (Transform):
    def __init__ (self):
        super().__init__(None)

    

    def process (self):
        df = pd.DataFrame(self.data)
        self.data = df
        return self
    


class PPreprocess (Transform):
    def __init__ (self):
        super().__init__(None)



    def correct_dtypes (self):
        df = pd.DataFrame(self.data)
        df["from"] = pd.to_datetime(df["from"])
        df["volume"] = df["volume"].astype(int)
        self.data = df
        return self

   

    def extract_rows_columns (self):
        cols = ["close", "high", "low", "open", "volume"]
        df = pd.DataFrame(self.data)
        df = df.sort_values(by=["from"])
        df = df.iloc[-1 * DAYS_TO_LEARN:]

        dates = df["from"].apply(lambda x: x+timedelta(3) if x.weekday() == 4 else x+timedelta(1))
        df = df[cols]
        self.data = {"date": dates, "data": df}
        return self



    def normalize (self):
        df = pd.DataFrame(self.data["data"])
        df = (df - df.mean()) / df.std()
        self.data["data"] = df
        return self
    


    def make_dataset (self):
        df = pd.DataFrame(self.data["data"])
        dataset = np.array(df)
        dataset = np.expand_dims(dataset, axis=0)
        self.data["data"] = dataset
        return self



    def process (self):
        self.correct_dtypes().extract_rows_columns().normalize().make_dataset()
        return self



class PPredict (Transform):
    def __init__ (self, path="./models/stock_prediction/lstm.keras"):
        self.path = path
        super().__init__(None)


    
    def predict (self):
        if not os.path.exists(self.path):
            raise FileNotFoundError(f"File not found at: {self.path}")
        
        try:
            predictor = tf.keras.models.load_model(self.path)
        except Exception as err:
            raise RuntimeError(f"Failed to load model: {err}")

        X_pred = self.data["data"]
        Y_pred = predictor.predict(X_pred)
        Y_pred = Y_pred[0].reshape(-1)

        self.data = pd.DataFrame({"date": self.data["date"], "close": Y_pred})
        return self
    


    def process (self):
        self.predict()
        return self
    


class PPostprocess (Transform):
    def __init__ (self, path=None, column_name="close"):
        self.path = path
        self.column_name = column_name
        super().__init__(None)



    def denormalize (self):
        df = FromDb("warehouse", "polygon_data", wpdschema, "from", DAYS_TO_LEARN + 1).run()
        mean = df[self.column_name].mean()
        std = df[self.column_name].std()

        df = pd.DataFrame(self.data)
        df[self.column_name] = (df[self.column_name] * std) + mean
        self.data = df

        return self



    def process (self):
        self.denormalize()
        return self