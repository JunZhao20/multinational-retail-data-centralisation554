import yaml
import pandas as pd
import tabula as tab
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
    
    def upload_to_db(self, data_frame, table_name):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = 'localhost'
        USER = 'postgres'
        PASSWORD = '97231987432'
        PORT = '5432'
        DATABASE = 'sales_data'
        sales_data_engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        sales_data_engine.connect()
        data_frame.to_sql(name=table_name, con=sales_data_engine, if_exists='replace', index=False)
        print('Success')
        
        

class DataExtractor(DatabaseConnector):
    def __init__(self):
        super().__init__()
        
    def list_db_tables(self):
        db_engine = super().init_db_engine(super().read_db_creds())
        metadata = MetaData()
        metadata.reflect(bind=db_engine)
        table_names = metadata.tables.keys()
        print('List of tables:')
        for table_name in table_names:
            print(table_name)
    
    def read_rds_table(self, table_name):
        df = pd.read_sql_table(table_name, con=self.init_db_engine)
        return df
    
    def retrieve_pdf_data(self, pdf_path):
        df = tab.read_pdf(pdf_path, pages='all')
        print(df)
        
        
        
try:
    db_conn = DatabaseConnector()
    engine = db_conn.init_db_engine
    pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    dataExtract = DataExtractor()
    df = dataExtract.retrieve_pdf_data(pdf_path)
    print(df)
    # extract = DataExtractor('legacy_users')
    # from data_cleaning import DataCleaning
    # data_cleaner = DataCleaning()
    # df = data_cleaner.clean_user_data()
    # upload = extract.upload_to_db(df, 'dim_users')
    # extract.read_rds_table()
    # extract.list_db_tables()
    
    # df.head()
except Exception as e:
    print(f'Error occurred in data_extraction: {e}')