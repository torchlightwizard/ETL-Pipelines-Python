import json
import logging
import os



class File ():
    def __init__ (self, path="", data={"data": "No data"}):
        self.path = path
        self.data = data
    


    def write (self):
        try:
            prev_data = []
            if os.path.exists(self.path) and os.path.getsize(self.path) > 0:
                try:
                    with open(self.path, "r", encoding="utf-8") as file:
                        prev_data = json.load(file)
                        if not isinstance(prev_data, (dict, list)):
                            raise ValueError(f"Non-List, Non-Dict data contained in file {self.path}.\n")
                        if isinstance(prev_data, dict):
                            prev_data = [prev_data]

                except Exception as err:
                    logging.info(f"Function: File.write(). Error: {err}\n")
                    prev_data = []

            prev_data.append(self.data)
            with open(self.path, "w", encoding="utf-8") as file:
                   json.dump(prev_data, file, indent=4)
                   return self

        except Exception as err:
            logging.exception(f"Function: File.write(). Error: {err}\n")
        return self
    


    def get (self):
        return self.data
    

    
    def set (self, data):
        if not isinstance(data, (str, dict, list)):
            raise TypeError(f"Function: File.set(). Failed to set data. Expected Str, Dict or List. Received DataType: {type(data)}\n")
        self.data = data
        return self