import io
import requests

import boto3
import pandas as pd
import tabula as tab
from database_utils import DatabaseConnector
from sqlalchemy import MetaData


class DataExtractor(DatabaseConnector):
    
    """
    This class stores the method that are used to extract the data from various sources. 
    
    Inheritance:
        DatabaseConnector : This allows the DataExtractor class to be initialise the database engine.
        
    """
    
    def __init__(self):
        super().__init__()
        
    # Displays all the db tables within the RDS 
    def list_db_tables(self):
        
        """
        This method displays all the database tables within the RDS
        """
        db_engine = super().init_db_engine(super().read_db_creds())
        metadata = MetaData()
        metadata.reflect(bind=db_engine)
        table_names = metadata.tables.keys()
        print('List of tables:')
        for table_name in table_names:
            print(table_name)
    
    
    def list_number_of_stores(self, end_point, header):
        
        """
        This method extracts a the number of stores that can be extract in the stores_data AWS api
        
        args:
            end_point (str) : It takes in an API.
            header (object) : Takes an object containing a key that allows access to the API.
        """
        
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
        
        """
        This method extracts the all the store_details from the api endpoint.
        
        args:
            store_number (int) : The number of stores to extract from the list_number_of_stores method
            header (object) : Takes an object containing a key that allows access to the API.
        """
        
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
        
        """
        This method extracts the database table to a pandas DataFrame.
        
        args:
            table_name (str) : Takes in the table_name of a table in the RDS
        """
        
        df = pd.read_sql_table(table_name, con=self.init_db_engine)
        return df
    
    def retrieve_pdf_data(self, pdf_path):
        
        """
        This method extracts the data from a pdf format into a pandas DataFrame.
        
        args:
            pdf_path (str) : This takes the pdf file path way.
        """
        
        extract_pdf = tab.read_pdf(pdf_path, pages='all', multiple_tables=True)
        df = pd.concat(extract_pdf, ignore_index=True)
        return df
        
    def extract_from_s3(self, address):
        
        """
        This method extracts data from a AWS S3 bucket into a pandas DataFrame.
        
        args:
            address (str) : Takes in a address of S3 bucket.
        """
        
        address_split = address.split('/')
        bucket = address_split[2]
        key = address_split[3]
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        df = pd.read_csv(io.BytesIO(content))
        df.to_csv('products.csv')
        
        
try:
    if __name__ == "__main__": 
        db_conn = DatabaseConnector()
        engine = db_conn.init_db_engine
        extract = DataExtractor()
       
    
    # df = pd.read_pickle('./cleaned_data/card_data.pkl')
    # upload = extract.upload_to_db(df, 'dim_card_details')
    
    
except Exception as e:
    print(f'Error occurred in data_extraction: {e}')