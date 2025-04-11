from src.step.step import BaseStep
from src.connect.db import Db

import os
import pandas as pd



class Extract (BaseStep):
    def __init__ (self, path, key, params, data):
        super().__init__()
        self.path = path
        self.key=key
        self.params = params
        self.data = data

    def get (self):
        return self

    def process (self):
        return self

    def run (self, data=None):
        self.get().process()
        return self.data
    


class FromCSV (Extract):
    def __init__ (self, path=None, chunksize=0):
        self.chunksize = chunksize
        super().__init__(path, None, None, None)



    def get (self):
        if self.path is None:
            raise SystemError(f"No path found.\n")

        if not os.path.exists(self.path):
            raise FileNotFoundError(f"File not found at: {self.path}")
        
        if self.path.split(".")[2] == "csv":
            prev_chunk = None
            for chunk in pd.read_csv(self.path, chunksize=self.chunksize):
                prev_chunk = self.data
                self.data = chunk
            self.data = pd.concat([prev_chunk, self.data])
            
        else:
            self.data = None

        return self



class FromDb (Extract):
    def __init__ (self, db=None, table=None, schema=[], order_column="", limit=0):
        self.db = db
        self.table = table
        self.schema = schema
        self.order_column = order_column
        self.limit = limit
        self.query = f"select * from (select * from {self.table} order by `{self.order_column}` desc limit {self.limit}) as q order by q.`{self.order_column}` asc;"
        self.data = None
        super().__init__(None, None, None, None)



    def get (self):
        array = Db(name=self.db, schema=self.schema).read(self.query)
        df = pd.DataFrame(array, columns=self.schema[0]["columns"].keys())
        self.data = df
        return self