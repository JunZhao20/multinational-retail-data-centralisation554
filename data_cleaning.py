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
        
        # Assigning correct Dtypes
        df.first_name = df.first_name.astype('string')
        df.last_name = df.last_name.astype('string')    
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce').dt.date
        df.date_of_birth = df.date_of_birth.astype('datetime64[ns]')
        df.company = df.company.astype('string')
        df.email_address = df.email_address.astype('string')
        df.address = df.address.astype('string')
        df.country = df.country.astype('string')
        df.country_code = df.country_code.astype('string')
        df.phone_number = df.phone_number.astype('string')
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce').dt.date
        df.join_date = df.join_date.astype('datetime64[ns]')
        # Applying regex to phone_number for readability and consistency
        df['phone_number'] = df['phone_number'].replace({r'\+44': '0',r'\+49': '0',r'\+1': '', r'\(': '', r'\)': '', r'-': '', r' ': ''}, regex=True)
        
        # Corrects typos
        df.loc[df['country_code'] == 'GGB', 'country_code'] = 'GB'
        
        # Correcting Index after dropping rows
        df.dropna(inplace=True)
        df.sort_values(by=['index'], ascending=True, inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.drop('index', axis=1, inplace=True)
        df['index'] = range(len(df))
        df.insert(0, 'index', df.pop('index'))
        
        df.to_feather('user_data.feather')
    
    def clean_card_data(self): 
        
        """
        This method is used to clean the extracted user card details data from a pdf file path. It then returns card_data.feather file
        """
        
        from data_extraction import DataExtractor, pdf_path
        extract = DataExtractor()
        df = extract.retrieve_pdf_data(pdf_path)
        
        # Assigning correct Dtypes
        df.expiry_date = pd.to_datetime(df.expiry_date,format='%m/%y', errors='coerce')
        df.card_provider = df.card_provider.astype('string')
        df.date_payment_confirmed = pd.to_datetime(df.date_payment_confirmed, errors='coerce')
        df.card_number = pd.to_numeric(df.card_number, errors='coerce')
        df.card_number = df.card_number.astype('int')
        
         # Correcting Index after dropping rows
        df.dropna(axis=0, inplace=True)
        df.reset_index(drop=True, inplace=True)

        df.to_feather('card_data.feather')
    
    # Cleans the Extracted stores_data from AWS S3 bucket
    def called_clean_store_data (self):
        
        """
        This method cleans the extracted stores_data from an object from a S3 bucket in a JSON format.
        This then returns a stores_data.feather file.
        """

        df = pd.read_json('./extracted_data/stores_data.json', lines=True)
        df.address = df.address.astype('string')
        df.address = df.address.str.replace(r'^\s+|\s+$|\n', ' ', regex=True)
        df.address = df.address.str.strip()
        df.drop(0, inplace=True)
        df.drop('lat', axis=1, inplace=True)
        df.replace('NULL', np.nan, inplace=True)
        df.longitude = pd.to_numeric(df.longitude, errors='coerce')
        df.locality = df.locality.astype('string')
        df.store_code = df.store_code.astype('string')
        df.staff_numbers = pd.to_numeric(df.staff_numbers, errors='coerce')
        df.dropna(axis=0, inplace=True)
        df['opening_date'] = df['opening_date'].apply(parse)
        df['opening_date'] = pd.to_datetime(df['opening_date'], infer_datetime_format=True, errors='coerce')
        df.store_type = df.store_type.astype('string')
        df.latitude = pd.to_numeric(df.latitude, errors='coerce')
        df.country_code = df.country_code.astype('string')
        df.continent = df.continent.astype('string')
        
        df.loc[df['continent'] == 'eeAmerica', 'continent'] = 'America'
        df.loc[df['continent'] == 'eeEurope', 'continent'] = 'Europe'

        df.reset_index(drop=True, inplace=True)
        
        df.drop('index', axis=1, inplace=True)
        df['index'] = range(len(df))
        df.insert(0, 'index', df.pop('index'))


        df.to_feather('stores_data.feather')

    def convert_product_weights(self, df):
        
        """
        This method is used to convert the weight column different unit types to represent the kg unit metric.
        It then returns the modified converted weight values to a new csv file 'product_unit_converted.csv'.
        
        
        args:
            df (series): this method receives a DataFrame argument to allow data manipulation to be preformed.
        
        """
        
        # Dropping NaN values from weight 
        df.dropna(subset=['weight'], inplace=True)
        df.weight = df.weight.astype('string')
       
        # Calculates mass value of products that contain multiple items
        df[['Multiplier', 'Mass']] = df['weight'].str.extract(r'(\d+) x (\d+)g')
        filtered_rows = df['weight'].str.contains(' x ')
        df.loc[filtered_rows, 'Total_Grams'] = pd.to_numeric(df.loc[filtered_rows, 'Multiplier']) * pd.to_numeric(df.loc[filtered_rows, 'Mass'])
        df.loc[filtered_rows, 'weight'] = df.loc[filtered_rows, 'Total_Grams'].astype(str) + 'g'
        df.drop(['Mass','Multiplier', 'Total_Grams'], axis=1, inplace=True)
        non_weight = df.weight.str.isupper()
        df.drop(df[non_weight].index, axis=0 ,inplace=True)
        
        # Remove string characters to convert into int to perform calculation for kg conversions
        df.weight = df.weight.str.replace('g', '')
        df.weight = df.weight.str.replace('ml', '')
        conversion_rows = df['weight'].str.isnumeric()
        df.loc[conversion_rows, 'weight'] = (pd.to_numeric(df.loc[conversion_rows, 'weight'])/1000).astype('str')
        df.weight = df.weight.str.replace('k', '')
        df.weight = pd.to_numeric(df.weight, errors='coerce')
        
        df.dropna(inplace=True)
        
        df.to_csv('product_unit_converted.csv')
    
    def clean_products_data(self):
        
        """
        This method cleans the extracted products data from the products_unit_converted.csv.
        It will then return the cleaned DataFrame file as 'products.feather'
        """
        
        df = pd.read_csv('./cleaned_data/product_unit_converted.csv')

        # Removing unwanted columns
        df = df.iloc[:, 2:]
        
        # Resetting Index
        df.reset_index(drop=True)
        
        # Correctly Assigning Dtypes
        df.product_name = df.product_name.astype('string')
        df.product_price = df.product_price.str.replace('£', '')
        df.product_price = pd.to_numeric(df.product_price)
        df.category = df.category.astype('string')
        df['date_added'] = df['date_added'].apply(parse)
        df['date_added'] = pd.to_datetime(df['date_added'], infer_datetime_format=True, errors='coerce')
        df.uuid = df.uuid.astype('string')
        df.removed = df.removed.astype('string')
        df.product_code = df.product_code.astype('string')
        
        df.to_feather('products.feather')
    
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
        
        # Assigning correct index
        df.reset_index(drop=True, inplace=True)
        df.drop('index', axis=1, inplace=True)
        df['index'] = range(len(df))
        df.insert(0, 'index', df.pop('index'))
        
        df.to_feather('orders_table.feather')
    
    def clean_date_times(self):
        
        """
        This method will clean the extracted data_time JSON from AWS S3. It will return a data_times.feather file. 
        """
        
        df = pd.read_json('./extracted_data/date_time.json')
        
        # Joining year, month and day to form a date column
        # df['date'] = df.year+ '-' + df.month + '-' + df.day
        # df.insert(1, 'date', df.pop('date'))
        # column_drop_year_month_date = ['month', 'year', 'day']
        # df.drop(columns=column_drop_year_month_date, inplace=True)
        
        # df.date = pd.to_datetime(df.date, errors='coerce')
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        # Correctly assigning Dtype
        df.timestamp = pd.to_datetime(df.timestamp, format='%H:%M:%S', errors='coerce').dt.time
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.time_period = df.time_period.astype('string')
        df.date_uuid = df.date_uuid.astype('string')
        
        # TODO : Upload and do task
        df.to_feather('date_times.feather')
        
        
    
try:
    # cleaner = DataCleaning()
    # cleaner.clean_date_times()
    
    # print(df.card_number.duplicated().sum())
    print(len('4999876853991480320'))
except Exception as e:
    print(f'Error Occurred in data_cleaning {e}')