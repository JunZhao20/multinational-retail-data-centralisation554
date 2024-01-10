import yaml
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, MetaData

class DatabaseConnector:
    
    def __init__(self):
        self.read_db_creds = self.read_db_creds()
        self.init_db_engine = self.init_db_engine(self.read_db_creds)
        
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            data = yaml.safe_load(file)
        return data
    
    def init_db_engine(self, db_creds):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = db_creds['RDS_HOST']
        USER = db_creds['RDS_USER']
        PASSWORD = db_creds['RDS_PASSWORD']
        PORT = db_creds['RDS_PORT']
        DATABASE = db_creds['RDS_DATABASE']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        engine.connect()
        return engine
        
        

class DataExtractor(DatabaseConnector):
    # def __init__(self):
    #     self.list_tables = self.list_db_tables()
    #     self.read_rds_table = self.read_rds_table()
        
    def list_db_tables(self):
        db_engine = super().init_db_engine(super().read_db_creds())
        metadata = MetaData()
        metadata.reflect(bind=db_engine)
        table_names = metadata.tables.keys()
        print('List of tables:')
        for table_name in table_names:
            print(table_name)
    
    def read_rds_table(self, engine , table_name):
        df = pd.read_sql_table(table_name, con=engine)
        print(df)
        
        
        
try:
    db_conn = DatabaseConnector()
    engine = db_conn.init_db_engine
    print(engine)
    extract = DataExtractor()
    extract.list_db_tables()
    # legacy_users data
    extract.read_rds_table(engine,'legacy_users')
    
    # print(db_init)
except Exception as e:
    print(f'Error occurred: {e}')