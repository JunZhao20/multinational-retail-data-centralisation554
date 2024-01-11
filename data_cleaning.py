import pandas as pd
from data_extraction import DataExtractor 

class DataCleaning:
        
    def clean_user_data(self):
        extractor = DataExtractor('legacy_users')
        df = extractor.read_rds_table()
        # df = df.drop(df.columns[0], axis=1)
        sort_indexes = df.sort_index()
        print(sort_indexes)
        # for i in df['country_code']:
        #     if len(i) > 2:
        #         print(i)
                
        
        # DataExtractor.read_rds_table()
        
cleaner = DataCleaning()

cleaner.clean_user_data()