from src.extract.extract import Extract
from src.connect.file import File

from polygon import RESTClient
from polygon.rest.models.aggs import DailyOpenCloseAgg

from dotenv import load_dotenv as env
import os
import logging
from time import sleep
from datetime import datetime
from datetime import timedelta

import pandas as pd



path_to_keys = "./keys/.env"
env(path_to_keys)



class PFetch (Extract):
    client = None

    def __init__ (self, key_id="POLYGON_KEY", days=10):
        self.days = days
        self.api_limit = 5

        key = os.getenv(key_id)
        if PFetch.client is None:
            PFetch.client = RESTClient(key)

        params = {
            "indicator": "ORCL",
            "date": datetime.now(),
            "adjusted": "true"
        }

        super().__init__(None, key, params, None)



    def get(self):
        responses = []
        for i in range(self.days):
            self.params["date"] = self.params["date"] - timedelta(1)
            try:
                if self.params["date"].weekday() in [5, 6]:
                    continue
                if (len(responses) > 0) and (len(responses) % self.api_limit == 0):
                    raise ConnectionAbortedError(f"Api limit reached. Currently loaded {len(responses)}/{self.days}. Will try again in 61 seconds.\n")
                
                response = self.client.get_daily_open_close_agg(self.params["indicator"], self.params["date"].strftime("%Y-%m-%d"), self.params["adjusted"])
                responses.append(response)

            except Exception as err:
                logging.info(f"Function: PFetch.get(). Error: {err}\n")
                responses.append({"status": "ERROR", "data": None})
                sleep(61)
                
        self.data = responses
        return self



    def process (self):
        processed = []
        for response in self.data:
            if not isinstance(response, DailyOpenCloseAgg):
                logging.info(f"Function: PFetch.process(). Received wrong response from api. Data type: {type(response)}\n")
                continue
            temp = response.__dict__
            temp = {feature.strip("_"): temp[feature] for feature in temp.keys()}
            processed.append(temp)
        self.data = processed
        return self