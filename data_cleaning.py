import pandas as pd
from IPython.display import display
from data_extraction import DataExtractor 

class DataCleaning:
        
    def clean_user_data(self):
        extractor = DataExtractor('legacy_users')
        df = extractor.read_rds_table()
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
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce').dt.date
        df.join_date = df.join_date.astype('datetime64[ns]')

        # print(df.dtypes)

        # print(df.isna().sum())
        try:
           for row in df.country_code:
               if len(row) > 2:
                   df.drop(df.loc[row])
                   print(row)
                  
        except Exception as e:
            print(f'Error has Occurred in Clean_user_data: {e}')
                
        
        # DataExtractor.read_rds_table()
        
cleaner = DataCleaning()

cleaner.clean_user_data()