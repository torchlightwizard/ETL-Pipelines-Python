from src.step.step import BaseStep
from src.connect.file import File
from src.connect.db import Db

from src.transform.transform import JsonWrap
from src.transform.transform import JsonUnWrap

import pandas as pd
import logging



class Load (BaseStep):
    def __init__ (self, path, params, data):
        super().__init__()
        self.path = path
        self.params = params
        self.data = data

    def set (self, data):
        self.data = data
        return self

    def load (self):
        return self

    def run (self, data=None):
        self.set(data).load()
        return self.data
    


class LoadToFile (Load):
    def __init__ (self, path=None):
        super().__init__(path, None, None)



    def load (self):
        try:
            if self.path is None:
                raise SystemError(f"No path found.\n")

            if isinstance(self.data, (pd.DataFrame, pd.Series)):
                try:
                    df = pd.read_csv(self.path+".csv")
                    if df.shape[0] > 0:
                        self.data.to_csv(self.path+".csv", index=False, mode="a", header=False)
                except Exception as err:
                    logging.info(f"Function: LoadToFile.load(). Failed to append data to original file. Will create new csv file. Error: {err}\n")
                    self.data.to_csv(self.path+".csv", index=False)


            elif isinstance(self.data, pd.Index):
                self.data = "\n".join(map(str, self.data))
                File(self.path+".csv").set(self.data).write()


            else:
                File(self.path+".json").set(self.data).write()

        except Exception as err:
            logging.exception(f"Function: LoadToFile.load(). Error: {err}\n")

        return self
    

class LoadToDb (Load):
    def __init__ (self, db=None, table=None, schema=[], column_wrapper="data"):
        self.db = db
        self.table = table
        self.schema = schema
        self.data = None
        self.column_wrapper = column_wrapper



    def load (self):
        if isinstance(self.data, (dict, list)):
            self.data = JsonWrap(self.column_wrapper).run(self.data)
        
        Db(name=self.db, schema=self.schema).write(self.table, self.data)

        if isinstance(self.data, dict):
            self.data = JsonUnWrap(self.column_wrapper).run(self.data)
        return self