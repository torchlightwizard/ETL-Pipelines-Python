from src.transform.transform import Transform

import os
from datetime import datetime
from datetime import timedelta
import json
import joblib

import pandas as pd
from sklearn.svm import SVC
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier
from sklearn.preprocessing import StandardScaler



class OMJsonToDF (Transform):
    def __init__ (self):
        super().__init__(None)

    

    def process (self):
        js = json.loads(self.data["DataFrame"])
        df = pd.DataFrame(js)
        df["latitude"] = self.data["Latitude"]
        df["longitude"] =  self.data["Longitude"]
        df["elevation"] =  self.data["Elevation"]
        self.data = df
        return self
    


class OMPreprocess (Transform):
    def __init__ (self):
        super().__init__(None)



    def correct_dtypes (self):
        df = pd.DataFrame(self.data)
        df["date"] = pd.to_datetime(df["date"], unit="ms")
        self.data = df
        return self
    
    

    def extract_daily (self):
        df = pd.DataFrame(self.data)
        df = df.sort_values("date", ascending=True)
        df = df[df["date"] < datetime.now()]
        df = df.iloc[-24:]
        self.data = df
        return self
    


    def extract_columns (self):
        cols = ["temperature_2m", "relative_humidity_2m", "dew_point_2m",
                "apparent_temperature", "precipitation", "cloud_cover_low",
                "cloud_cover_mid", "cloud_cover_high", "wind_speed_10m",
                "wind_speed_100m", "wind_direction_10m", "wind_direction_100m",
                "wind_gusts_10m", "et0_fao_evapotranspiration",
                "vapour_pressure_deficit"]
        
        df = pd.DataFrame(self.data)
        next_day = df["date"].max() + timedelta(1)
        next_day = datetime(next_day.year, next_day.month, next_day.day)
        df = df[cols]
        self.data = {"date":next_day, "data": df}
        return self
    


    def squeeze (self):
        df = pd.DataFrame(self.data["data"])
        df = df.mean()
        df = pd.DataFrame(df).T
        self.data["data"] = df
        return self
    


    def normalize (self):
        path_model = "./models/rainfall_forecast/"
        scaler = joblib.load(path_model+"open_meteo_scaler.pkl")

        df = pd.DataFrame(self.data["data"])
        df = scaler.transform(df)
        self.data["data"] = df
        return self



    def process (self):
        self.correct_dtypes().extract_daily().extract_columns().squeeze().normalize()
        return self



class OMPredict (Transform):
    def __init__ (self, path="./models/rainfall_forecast/open_meteo_classifier.pkl"):
        self.path = path
        super().__init__(None)


    
    def predict (self):
        if not os.path.exists(self.path):
            raise FileNotFoundError(f"File not found at: {self.path}")
        
        try:
            classifier = joblib.load(self.path)
        except Exception as err:
            raise RuntimeError(f"Failed to load model: {err}")

        X_pred = self.data["data"]
        Y_pred = classifier.predict(X_pred)

        self.data = pd.DataFrame({"date": [self.data["date"]], "rain": Y_pred})
        return self



    def process (self):
        self.predict()
        return self