import pandas as pd
import numpy as np
from dateutil.parser import parse

class DataCleaning:
    
    
    def clean_user_data(self):
        
        """
        This method is used to clean the extracted user data from the AWS RDS table called 'legacy_users', it then returns a
        user_data.feather file.
        
        """
        
        from data_extraction import DataExtractor 
        extractor = DataExtractor()
        df = extractor.read_rds_table('legacy_users')
        
        df = df[~df.user_uuid.str.isupper()]
        try:
            df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='%Y-%B-%d')
            df['join_date'] = pd.to_datetime(df['join_date'], format='%Y-%B-%d')

        except Exception as e:
            try:
                df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='%Y-%B-%d').dt.strftime('%Y-%m-%d')
                df['join_date'] = pd.to_datetime(df['join_date'], format='%Y-%B-%d').dt.strftime('%Y-%m-%d')

            except Exception as e:
                print(f'Error has occured within clean_user_test : {e}')
                
        df.phone_number = df.phone_number.astype('string')

        df['phone_number'] = df['phone_number'].replace({r'\+44': '0',r'\+49': '0',r'\+1': '', r'\(': '', r'\)': '', r'-': '', r'\.':'' ,r' ': ''}, regex=True)
        df.loc[df['country_code'] == 'GGB', 'country_code'] = 'GB'
        df.sort_values(by=['index'], ascending=True, inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.drop('index', axis=1, inplace=True)
        df['index'] = range(len(df))
        df.insert(0, 'index', df.pop('index'))
        
        
        df.to_feather('./cleaned_data/user_data.feather')
    
        
    def clean_card_data(self): 
        
        """
        This method is used to clean the extracted user card details data from a pdf file path. It then returns card_data.feather file
        """
    
        from data_extraction import DataExtractor
        extract = DataExtractor()
        pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        df = extract.retrieve_pdf_data(pdf_path)
        
        df = df.drop_duplicates(subset=['card_number'])
        df.card_number = df.card_number.astype('string')
        df.expiry_date = df.expiry_date.astype('string')
        df['card_number'] = df['card_number'].replace({r'\.':'', r'\?':'' }, regex=True)
        exp_null = df.index[df.expiry_date.str.isalpha()].tolist()
    
        null_list = [377, 827]
        df.drop([377,827], inplace=True)
        card_null = df.index[df.card_number.isna()].tolist()
        df.card_number = pd.to_numeric(df.card_number, errors='coerce')

        df.dropna(inplace=True)
        
        df.reset_index(drop=True, inplace=True)

        df.to_feather('./cleaned_data/card_data.feather')
    
    # Cleans the Extracted stores_data from AWS S3 bucket
    def clean_store_data (self):
        
        """
        This method cleans the extracted stores_data from an object from a S3 bucket in a JSON format.
        This then returns a stores_data.feather file.
        """

        df = pd.read_json('./extracted_data/stores_data.json', lines=True)
        df = df.drop_duplicates(subset=['store_code'])
        df.drop('lat', axis=1, inplace=True)
        df.longitude = pd.to_numeric(df.longitude, errors='coerce')
        
        df.address = df.address.astype('string')
        df.address = df.address.str.replace(r'^\s+|\s+$|\n', ' ', regex=True)
        df.address = df.address.str.strip()

        
        df.staff_numbers = pd.to_numeric(df.staff_numbers, errors='coerce')
        df['staff_numbers'].fillna(df['staff_numbers'].median(), inplace=True)
       
        df['opening_date'] = pd.to_datetime(df['opening_date'], infer_datetime_format=True, errors='coerce')
        df['opening_date'].fillna(method='ffill', inplace=True)
        
        df.continent = df.continent.astype('string')
        
        df.loc[df['continent'] == 'eeAmerica', 'continent'] = 'America'
        df.loc[df['continent'] == 'eeEurope', 'continent'] = 'Europe'
        
        df.drop(df[df['continent'] == 'NULL'].index, inplace=True)
        
        # filtered_df = df[~df['store_code'].str.contains('-')]
        
        non_store_codes = [63, 172, 231, 447, 414, 381, 333]
        
        df.drop(non_store_codes, inplace=True)

        df.reset_index(drop=True, inplace=True)
        df.drop('index', axis=1, inplace=True)
        df['index'] = range(len(df))
        df.insert(0, 'index', df.pop('index'))
        print(df.continent.unique())
        
        df.to_feather('./cleaned_data/store_data.feather')

    def convert_product_weights(self, df):
        
        """
        This method is used to convert the weight column different unit types to represent the kg unit metric.
        It then returns the modified converted weight values to a new csv file 'product_unit_converted.csv'.
        
        
        args:
            df (series): this method receives a DataFrame argument to allow data manipulation to be preformed.
        
        """
        # Dropping NaN values from weight 

        df.weight = df.weight.astype('string')
        
        # Calculates mass value of products that contain multiple items
        df[['Multiplier', 'Mass']] = df['weight'].str.extract(r'(\d+) x (\d+)g')
        filtered_rows = df['weight'].str.contains(' x ')
        df.loc[filtered_rows, 'Total_Grams'] = pd.to_numeric(df.loc[filtered_rows, 'Multiplier']) * pd.to_numeric(df.loc[filtered_rows, 'Mass'])
        df.loc[filtered_rows, 'weight'] = df.loc[filtered_rows, 'Total_Grams'].astype(str) + 'g'
        df.drop(['Mass','Multiplier', 'Total_Grams'], axis=1, inplace=True)
        non_weight = df.weight.str.isupper()
        df.drop(df[non_weight].index, axis=0 ,inplace=True)
        
        # null_indices = df.index[df.isnull().any(axis=1)].tolist()
        
        null_val = [266, 788, 794, 1660]
        df.drop(null_val, inplace=True)
        
        # Remove string characters to convert into int to perform calculation for kg conversions
        df.weight = df.weight.str.replace('g', '')
        df.weight = df.weight.str.replace(' .', '')
        df.weight = df.weight.str.replace('ml', '')
        conversion_rows = df['weight'].str.isnumeric()
        df.loc[conversion_rows, 'weight'] = (pd.to_numeric(df.loc[conversion_rows, 'weight'])/1000).astype('str')
        df.weight = df.weight.str.replace('k', '')
        df.weight = df.weight.str.replace('oz', '')
        df['weight'].loc[1841] = '0.453592'
        df.weight = pd.to_numeric(df.weight, errors='coerce')
        

        df.to_csv('new_product_unit_converted.csv')
        
    
    def clean_products_data(self):
        
        """
        This method cleans the extracted products data from the products_unit_converted.csv.
        It will then return the cleaned DataFrame file as 'products.feather'
        """
        
        df = pd.read_csv('./cleaned_data/new_product_unit_converted.csv')

        # Removing unwanted columns
        df = df.iloc[:, 3:]
        
        # Resetting Index
        df.reset_index(drop=True)
        
        # Correctly Assigning Dtypes
        df.product_name = df.product_name.astype('string')
        df.category = df.category.astype('string')
        df['date_added'] = df['date_added'].apply(parse)
        df['date_added'] = pd.to_datetime(df['date_added'], infer_datetime_format=True, errors='coerce')
        df.uuid = df.uuid.astype('string')
        df.removed = df.removed.astype('string')
        df.product_code = df.product_code.astype('string')
        
        df.to_feather('./cleaned_data/products.feather')
    
    def clean_orders_data(self):
        
        """
        This method cleans the extracted data from the AWS RDS order_table. It will return order_table.feather. 
        """
        
        df = pd.read_pickle('./extracted_data/orders_table.pkl')

        # Dropping unwanted columns
        drop_columns = ['first_name', 'last_name', '1', 'level_0']
        df.drop(columns=drop_columns, inplace=True)
        

        # Correctly assigns Dtypes
        df.date_uuid = df.date_uuid.astype('string')

        df.user_uuid = df.user_uuid.astype('string')

        df.store_code = df.store_code.astype('string')

        df.product_code = df.product_code.astype('string')
        
        
        df.to_feather('./cleaned_data/new_orders_table.feather')
    
    def clean_date_times(self):
        
        """
        This method will clean the extracted data_time JSON from AWS S3. It will return a data_times.feather file. 
        """
        
        df = pd.read_json('./extracted_data/date_time.json')
        
       
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        # Correctly assigning Dtype
        df.timestamp = pd.to_datetime(df.timestamp, format='%H:%M:%S', errors='coerce').dt.time
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.time_period = df.time_period.astype('string')
        df.date_uuid = df.date_uuid.astype('string')
        
        
        df.to_feather('date_times.feather')
        
        
    
try:
    cleaner = DataCleaning()
   
    
except Exception as e:
    print(f'Error Occurred in data_cleaning {e}')