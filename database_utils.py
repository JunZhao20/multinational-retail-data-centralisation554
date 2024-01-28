
import yaml
import pandas as pd
from sqlalchemy import create_engine

class DatabaseConnector:
    """
    This class is used to connect to the RDS database and the local database
    
    Attributes:
        read_db_creds: retrieves and stores the db_creds of the RDS database within a yaml file.
        init_db_engine: stores the RDS engine.
    """
    def __init__(self):
        self.read_db_creds = self.read_db_creds()
        self.init_db_engine = self.init_db_engine(self.read_db_creds)
        
    # Load the RDS db credentials
    def read_db_creds(self):
        """
        This method loads the db credentials from db_creds.yaml 
        """
        with open('db_creds.yaml', 'r') as file:
            data = yaml.safe_load(file)
        return data
    
    # Initialise db engine using the RDS db credentials
    def init_db_engine(self, db_creds):
        """
        This method initializes and returns the RDS engine.
        
        args:
            db_creds (str) : the retrieved db credentials from the read_db_creds method.
        
        """
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
    
    # Upload cleaned dataframe to the sales_data db
    def upload_to_db(self, dataframe, table_name):
        
        """
        This method is used to upload the cleaned DataFrame to the sales_data database (local database).
        
        args:
            data_frame (series) : Takes in a dataframe to upload data from.
            table_name (str) : Takes in the name of the table you wish to name it.
        """
        
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = 'localhost'
        USER = 'postgres'
        PASSWORD = '97231987432'
        PORT = '5432'
        DATABASE = 'sales_data'
        sales_data_engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        with sales_data_engine.connect():
            dataframe.to_sql(name=table_name, con=sales_data_engine, if_exists='replace', index=False)
            print(f'Success {table_name} has been uploaded')
            
try:
    extract = DatabaseConnector()
    df = pd.read_feather('./cleaned_data/date_times.feather')
    upload = extract.upload_to_db(df, 'dim_date_times')
    
except Exception as e:
    print(f'Error in database_utils.py {e}')
    