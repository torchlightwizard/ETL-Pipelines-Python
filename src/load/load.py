from src.step.step import BaseStep
from src.connect.file import File
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

    def output (self):
        return self

    def run (self, data=None):
        self.set(data).output()
        return self.data
    


class LoadToFile (Load):
    def __init__ (self, path=None):
        super().__init__(path, None, None)



    def output (self):
        try:
            if self.path is None:
                raise SystemError(f"No path found.\n")

            if isinstance(self.data, (pd.DataFrame, pd.Series)):
                try:
                    df = pd.read_csv(self.path+".csv")
                    if df.shape[0] > 0:
                        self.data.to_csv(self.path+".csv", index=False, mode="a", header=False)
                except Exception as err:
                    logging.info(f"Function: LoadToFile.output(). Failed to append data to original file. Will create new csv file. Error: {err}\n")
                    self.data.to_csv(self.path+".csv", index=False)


            elif isinstance(self.data, pd.Index):
                self.data = "\n".join(map(str, self.data))
                File(self.path+".csv").set(self.data).write()


            else:
                File(self.path+".json").set(self.data).write()

        except Exception as err:
            logging.exception(f"Function: LoadToFile.output(). Error: {err}\n")

        return self