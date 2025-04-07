from src.step.step import BaseStep
import logging
from datetime import datetime
import json

import pandas as pd



class Transform (BaseStep):
    def __init__ (self, data):
        super().__init__()
        self.data = data

    def set (self, data):
        self.data = data
        return self

    def process (self):
        return self

    def run (self, data=None):
        self.set(data).process()
        return self.data
    
    

class Log (Transform):
    def __init__ (self, verbose=False):
        self.verbose = verbose
        super().__init__(None)

    

    def process (self):
        if self.verbose == True:
            logging.info("Data:\n")
            logging.info(self.data)
        logging.info(f"Data Type: {type(self.data)}\n")
        return self
    

class Sort (Transform):
    def __init__ (self, column_name=None, column_type=datetime, ascending=True):
        self.column_name = column_name
        self.column_type = column_type
        self.ascending = ascending
        super().__init__(None)

    def process (self):
        df = self.data
        if (self.column_name is not None) and isinstance(df, pd.DataFrame):
            if issubclass(self.column_type, datetime):
                df[self.column_name] = pd.to_datetime(df[self.column_name])
            else:
                df[self.column_name] = df[self.column_name].astype(self.column_type)
            self.data = df.sort_values(by=[self.column_name], ascending=self.ascending)
        else:
            logging.info(f"Function: Sort.process(): Data is not of type pd.DataFrame, instead its {type(df)}.\n")
        
        return self
    


class JsonWrap (Transform):
    def __init__ (self, column_name=None):
        self.column_name = column_name
        super().__init__(None)


    def process (self):
        self.data = {
            self.column_name: json.dumps(self.data)
        }
        return self
    


class JsonUnWrap (Transform):
    def __init__ (self, column_name=None):
        self.column_name = column_name
        super().__init__(None)


    def process (self):
        self.data = json.loads(self.data[self.column_name])
        return self