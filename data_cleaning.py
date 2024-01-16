import pandas as pd

class DataCleaning:
    def __init__(self):
        self.cleaned_user_data = self.clean_user_data()
        self.cleaned_card_data = self.clean_card_data()
        
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
        
        return df
    
    def clean_card_data(self):
        from data_extraction import DataExtractor, pdf_path
        extractor = DataExtractor()
        df = extractor.retrieve_pdf_data(pdf_path)

        df.expiry_date = pd.to_datetime(df.expiry_date,format='%m/%y', errors='coerce')
        df.card_provider = df.card_provider.astype('string')
        df.date_payment_confirmed = pd.to_datetime(df.date_payment_confirmed, errors='coerce')
        df.card_number = pd.to_numeric(df.card_number, errors='coerce')
        df.dropna(axis=0, inplace=True)
        df.card_number = df.card_number.astype('int')

        return df        
        
try:
    cleaner = DataCleaning()
    cleaner.clean_card_data()
except Exception as e:
    print(f'Error Occurred in data_cleaning {e}')