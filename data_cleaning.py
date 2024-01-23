import pandas as pd
import numpy as np

class DataCleaning:
        
    def clean_user_data(self):
        from data_extraction import DataExtractor 
        extractor = DataExtractor()
        df = extractor.read_rds_table('legacy_users')
        df.sort_values(by=['index'], ascending=True, inplace=True)
        df.set_index('index', inplace=True)
        df.reset_index(inplace=True)
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
        df['phone_number'] = df['phone_number'].replace({r'\+44': '0',r'\+49': '0',r'\+1': '', r'\(': '', r'\)': '', r'-': '', r' ': ''}, regex=True)
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce').dt.date
        df.join_date = df.join_date.astype('datetime64[ns]')
        df.dropna(inplace=True)
        df.loc[df['country_code'] == 'GGB', 'country_code'] = 'GB'
        df.to_pickle('user_data.pkl')
        
    def clean_card_data(self): 
        from data_extraction import DataExtractor, pdf_path
        extract = DataExtractor()
        df = extract.retrieve_pdf_data(pdf_path)
        df.expiry_date = pd.to_datetime(df.expiry_date,format='%m/%y', errors='coerce')
        df.card_provider = df.card_provider.astype('string')
        df.date_payment_confirmed = pd.to_datetime(df.date_payment_confirmed, errors='coerce')
        df.card_number = pd.to_numeric(df.card_number, errors='coerce')
        df.dropna(axis=0, inplace=True)
        df.card_number = df.card_number.astype('int')
        
        df.to_pickle('card_data.pkl')
    
    def called_clean_store_data (self):
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
        from dateutil.parser import parse
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


        df.to_pickle('stores_data.pkl')

    def convert_product_weights(self, df):
        
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
        

        df.weight = df.weight.str.replace('g', '')
        df.weight = df.weight.str.replace('ml', '')
        
        conversion_rows = df['weight'].str.isnumeric()
        df.loc[conversion_rows, 'weight'] = (pd.to_numeric(df.loc[conversion_rows, 'weight'])/1000).astype('str')
        df.weight = df.weight.str.replace('k', '')
        df.weight = pd.to_numeric(df.weight, errors='coerce')
        df.dropna(inplace=True)
        df.to_csv('product_unit_converted.csv', index=False)

        # return df
    
    def clean_products_data(self):
        pass
    
    
try:
    cleaner = DataCleaning()
    df = pd.read_csv('./extracted_data/products.csv')
    cleaner.clean_products_data()
except Exception as e:
    print(f'Error Occurred in data_cleaning {e}')