import mysql.connector as mysqlconnector
from dotenv import load_dotenv as env
import subprocess
import json
import logging
import os
import shutil
import subprocess

import pandas as pd



path_to_keys = "./keys/.env"
env(path_to_keys)




class Db ():

    def __init__ (self,user, password, host, name, schema, path=None, data=None):
        self.user = user
        self.password = password
        self.host = host

        self.connection = None

        self.name = name # database name
        self.schema = schema

        self.path = path
        self.data = data



    def connect (self):
        if self.connection is not None:
            return self
        
        self.connection = mysqlconnector.connect(
            user=self.user,
            password=self.password,
            host=self.host
        )
        logging.info(f"Successfully started connection to db: {self.name}")
        return self
    


    def close (self):
        if self.connection is None:
            return self
        
        try:
            self.connection.commit()
        except:
            pass

        try:
            self.connection.close()
        except:
            pass

        self.connection = None
        logging.info(f"Successfully closed connection to db: {self.name}")
        return self



    def __del__(self):
        self.close()
    


    def existsdb (self):
        with self.connection.cursor() as cursor:
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()

            if (self.name,) in databases:
                return True
            else:
                return False
        


    def existstable (self, table):
        with self.connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = '{self.name}' AND table_name = '{table}'
            """)
            result = cursor.fetchone()

            if result[0] > 0:
                return True
            else:
                return False
    


    def newdb (self):
        with self.connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {self.name}")
            cursor.execute(f"USE {self.name}")
        return self
    


    def newtable (self, schema):
        with self.connection.cursor() as cursor:
            cursor.execute(f"USE {self.name}")

            table_name = schema.get('table_name')
            columns = schema.get('columns', {})

            if not table_name or not columns:
                raise ValueError("Schema must contain a 'table_name' and 'columns'.")

            columns_sql = []
            for column_name, column_type in columns.items():
                columns_sql.append(f"`{column_name}` {column_type}")

            create_table_query = f"CREATE TABLE `{table_name}` (\n  {',\n  '.join(columns_sql)}\n);"

            cursor.execute(create_table_query)
        return self



    def query (self, query):
        with self.connection.cursor() as cursor:
            cursor.execute(f"USE {self.name}")

            cursor.execute(query)

            if query.strip().lower().startswith("select"):
                self.data = cursor.fetchall()
            else:
                self.connection.commit()

        return self
    


    def insert (self, table):
        with self.connection.cursor() as cursor:
            cursor.execute(f"USE {self.name}")

            if isinstance(self.data, dict):
                columns = ", ".join(self.data.keys())
                placeholders = ", ".join(["%s"] * len(self.data))
                values = tuple(self.data.values())

                query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                cursor.execute(query, values)
                self.connection.commit()
            
            elif isinstance(self.data, (pd.DataFrame, pd.Series)):
                df = self.data
                df = df.where(pd.notnull(df))

                columns = ", ".join(df.columns)
                placeholders = ", ".join(["%s"] * len(df.columns))
                query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

                data = [tuple(row) for row in df.itertuples(index=False, name=None)]
                cursor.executemany(query, data)

                self.connection.commit()

            else:
                raise TypeError(f"Function: Db.insert(). Unsupported data type. Insert failed.")

            self.data = None
        
        return self
        
    

    def exportdb (self):
        if shutil.which("mysqldump") is None:
            raise FileNotFoundError("Function: Db.exportdb(). mysqldump command not found in PATH.")
    
        try:
            command = f"mysqldump -u {self.user} -p{self.password} {self.name} > {self.path}"
            subprocess.run(command, shell=True, check=True)
        except subprocess.CalledProcessError as err:
            raise RuntimeError(f"Function: Db.exportdb(). Error exporting db: {err}")
        
        logging.info(f"Successfully exported db {self.name}")
        return self



    def importdb (self):
        if shutil.which("mysql") is None:
            raise FileNotFoundError("Function: Db.importdb(). mysql command not found in PATH.")
    
        try:
            command = f"mysql -u {self.user} -p{self.password} {self.name} < {self.path}"
            subprocess.run(command, shell=True, check=True)
        except subprocess.CalledProcessError as err:
            raise RuntimeError(f"Function: Db.importdb(). Error importing db: {err}")
        
        logging.info(f"Successfully imported db {self.name}")
        return self
    


    def set (self, data=None):
        self.data = data
        return self



    def configdb (self):
        if self.existsdb():
            return self
        
        if self.path:
            if not os.path.exists(self.path):
                raise FileNotFoundError(f"Function: Db.configdb(). Invalid path {self.path}. Please provide valid backup path.")
            return self.importdb()
        
        return self.newdb()
        

    
    def configtables (self):
        for table in self.schema.keys():
            if not self.existstable(table):
                self.newtable(self.schema[table])
        return self
    


    def read (self, query):
        try:
            self.connect().conifgdb().configtables().query(query).close()
        except Exception as err:
            logging.error(f"Function: Db.read(). Error {err}")



    def write (self, table):
        try:
            self.connect().conifgdb().configtables().insert(table).close()
        except Exception as err:
            logging.error(f"Function: Db.write(). Error {err}")