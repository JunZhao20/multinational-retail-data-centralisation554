import yaml
import psycopg2
from sqlalchemy import create_engine


class DataExtractor:
    pass

class DatabaseConnector:
    
    def __init__(self):
        self.read_creds = self.read_db_creds()
        self.db_engine = self.init_db_engine(self.read_creds)
        self.list_table = self.list_db_tables(self.db_engine)
        
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            data = yaml.safe_load(file)
        return data
    
    def init_db_engine(self, data):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = data['RDS_HOST']
        USER = data['RDS_USER']
        PASSWORD = data['RDS_PASSWORD']
        PORT = data['RDS_PORT']
        DATABASE = data['RDS_DATABASE']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        engine.connect()
        
    def list_db_tables(self, database):
        for table in database:
            print(table)
            
        
try:
    db_conn = DatabaseConnector()
    db_init = db_conn.init_db_engine
    show_table = db_conn.list_db_tables
    print(db_init)
except Exception as e:
    print(f'Error occurred: {e}')