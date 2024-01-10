import yaml
import psycopg2
from sqlalchemy import create_engine, MetaData

class DatabaseConnector:
    
    def __init__(self):
        self.read_creds = self.read_db_creds()
        self.db_engine = self.init_db_engine(self.read_creds)
        # self.list_table = self.list_db_tables(self.db_engine)
        
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
        return engine
        
        

class DataExtractor(DatabaseConnector):
    def __init__(self):
        self.list_tables = self.list_db_tables()
        
    def list_db_tables(self):
        db_engine = super().init_db_engine(super().read_db_creds())
        metadata = MetaData()
        metadata.reflect(bind=db_engine)
        table_names = metadata.tables.keys()
        print('List of tables:')
        for table_name in table_names:
            print(table_name)
        
        
        
       
        
try:
    # db_conn = DatabaseConnector()
    
    # db_conn.init_db_engine
    # show_table = db_conn.list_db_tables
    instance = DataExtractor()
    instance.list_tables
    # print(db_init)
except Exception as e:
    print(f'Error occurred: {e}')