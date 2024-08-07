import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os
import psycopg2


class LoadFiles():

    def __init__(self, schema:str, table:str, path:str):
        self.schema = schema
        self.table = table
        self.path = path
        self.conn = None
    
    def create_connect(self):
        try:
            engine = create_engine('''postgresql://melhorenvio:melhorenvio123@db:5432/melhorenvio''')
            self.conn = engine.connect()
        except Exception as e:
            print(f'Falha ao estabeler conexão com banco de dados - create_connect: {e}')

    def close_connect(self):
        try:
            if self.conn is not None:
                if self.conn.closed == False:
                    self.conn.close()
        except Exception as e:
            print(f'Erro ao fechar conexão: {e}')


    def select_table(self, query=None):
            try:
                self.create_connect()
                if query is None:
                    return pd.read_sql(text(f'select * from {self.schema}.{self.table}'), self.conn)
                else:
                    return pd.read_sql(text(query), self.conn)
            except Exception as e:
                print(f'Erro ao fechar conexão: {e} - select_table')
            finally:
                self.close_connect()
    
    def execute_query(self, query):
        try:
            conn = psycopg2.connect(database="melhorenvio", 
                                    user='melhorenvio', 
                                    password='melhorenvio123', 
                                    host='db', 
                                    port='5432') 
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(query) 
            conn.commit() 
        except Exception as e:
            print(f'Falha ao realizar consulta no banco de dados: - execute_query{e}')
        finally:
            conn.close() 

    def query(self, query):
        try:
            self.create_connect()
            return pd.read_sql_query(text(query), self.conn)
        except Exception as e:
            print(f'Falha ao realizar consulta no banco de dados: - query {e}')
            return None
        finally:
            self.close_connect()

    def to_table(self, df, schema=None, table=None):
        try:
            self.create_connect()
            df.to_sql(self.table, schema=self.schema, con=self.conn, if_exists='append', index=False)
            return True
        except Exception as e:
            print(f'Falha ao realizar consulta no banco de dados: to_table- {e}')
            return False
        finally:
            self.close_connect()
