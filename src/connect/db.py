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

    def __init__ (self, user="DB_USER", password="DB_PASS", host="DB_HOST", name="", path=None, schema=[], data=None):
        self.user = os.getenv(user)
        self.password = os.getenv(password)
        self.host = os.getenv(host)

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
        logging.info(f"Successfully started connection to db: {self.connection}")
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
        logging.info(f"Successfully closed connection to db: {self.connection}")
        return self



    def __del__(self):
        return self.close()
    


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
            

    
    def existsschema (self, table):
        with self.connection.cursor() as cursor:
            cursor.execute(f"USE {self.name}")
            cursor.execute(f"DESCRIBE {table['table_name']}")

            required_schema = table["columns"]
            actual_schema = {row[0]: row[1].split('(')[0] for row in cursor.fetchall()}

            for col, dtype in required_schema.items():
                dtype = dtype.split(" ")[0].lower()
                if actual_schema.get(col) != dtype:
                    raise KeyError(f"Function: Db.existsschema(). The database {self.name} has schema mismatch in table {table['table_name']}: {col} should be {dtype}, found {actual_schema.get(col)}")
        return self
    


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

            create_table_query = f"CREATE TABLE `{table_name}` (  {',  '.join(columns_sql)});"
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
                columns = [f"`{column}`" for column in self.data.keys()]
                columns = ", ".join(columns)
                placeholders = ", ".join(["%s"] * len(self.data))
                values = tuple(self.data.values())

                query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                cursor.execute(query, values)
                self.connection.commit()
            
            elif isinstance(self.data, (pd.DataFrame, pd.Series)):
                df = self.data
                df = df.where(pd.notnull(df), None)

                columns = [f"`{column}`" for column in df.columns]
                columns = ", ".join(columns)
                placeholders = ", ".join(["%s"] * len(df.columns))
                data = [tuple(None if pd.isna(x) else x for x in row) for row in df.itertuples(index=False, name=None)]
                query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
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
    


    def get(self):
        return self.data



    def configdb (self):
        if self.existsdb():
            return self
        
        self.newdb()

        if self.path and os.path.exists(self.path):
            self.importdb()
        return self

    
    def configtables (self):
        for table in self.schema:
            if self.existstable(table["table_name"]):
                self.existsschema(table)
            else:
                self.newtable(table)
        return self
    


    def read (self, query):
        try:
            return self.connect().configdb().configtables().query(query).close().get()
        except Exception as err:
            logging.error(f"Function: Db.read(). Error {err}")
        return self



    def write (self, table, data):
        try:
            return self.connect().configdb().configtables().set(data).insert(table).close()
        except Exception as err:
            logging.error(f"Function: Db.write(). Error {err}")
        return self