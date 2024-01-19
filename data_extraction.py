import yaml
import pandas as pd
import tabula as tab
import requests
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
        with sales_data_engine.connect():
            data_frame.to_sql(name=table_name, con=sales_data_engine, if_exists='replace', index=False)
            print(f'Success {table_name} has been uploaded')
        
        

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
    
    def list_number_of_stores(self, end_point, header):
        response = requests.get(end_point, headers=header)
        if response.status_code == 200:
            data = response.json()
            print('Success')
            num_stores = data['number_stores']
            print(num_stores)
            return num_stores
        else:
            print(f'Request failed with status code: {response.status_code}')
            print(f'Response text: {response.text}')
            
    def retrieve_stores_data(self,store_number, header):
        json_data = []
        for num in range(store_number+1):
            store_data_endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{num}'
            response = requests.get(store_data_endpoint,headers=header)
            if response.status_code == 200:
                data = response.json()
                json_data.append(data)
                print(data)
                
            else:
                print(f'error {response.status_code}, {response.text}')
        
        df = pd.json_normalize(json_data)
        df.to_json('stores_data.json', orient='records', lines=True)
        print(df)
        return df
        
    
    def read_rds_table(self, table_name):
        df = pd.read_sql_table(table_name, con=self.init_db_engine)
        return df
    
    def retrieve_pdf_data(self, pdf_path):
        extract_pdf = tab.read_pdf(pdf_path, pages='all', multiple_tables=True)
        df = pd.concat(extract_pdf, ignore_index=True)
        return df
        
    
        
try:
    db_conn = DatabaseConnector()
    engine = db_conn.init_db_engine
    pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    extract = DataExtractor()
    # header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    # num_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    # store_number = extract.list_number_of_stores(num_store_endpoint, header)
    # df_store_data = extract.retrieve_stores_data(store_number, header)
    # from data_cleaning import DataCleaning
    # data_cleaner = DataCleaning()
    # df = data_cleaner.clean_card_data()
    df = pd.read_pickle('./cleaned_data/stores_data.pkl')
    upload = extract.upload_to_db(df, 'dim_store_details')
    
    
    # df.head()
except Exception as e:
    print(f'Error occurred in data_extraction: {e}')