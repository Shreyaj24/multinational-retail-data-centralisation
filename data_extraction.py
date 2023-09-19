from database_utils import DatabaseConnector
import pandas as pd
import tabula as tb
import boto3
import requests

#from data_cleaning import DataCleaning
#import database_utils
#Class definition
class DataExtractor():
    '''
    This class works as a utility class, in it we create methods that help extract data from different data sources.
    The methods contained extract data from a particular data source, these sources include CSV files, an API and an S3 bucket.
    '''
    #class constructor
    def __init__(self):
        #attributes
        self.legacy_df = None
        self.combined_df = None
        self.store_df = None
        #self.num_stores = None
    #method
    def read_rds_table(self, datacon : DatabaseConnector, table_name : str):
        #table_rds = DatabaseConnector()
        list_tables = datacon.list_db_tables()
        #print(datacon.table_list)
        #for table in :
        if table_name in list_tables:
            sql_query = f'SELECT * from {table_name}'
            pd.set_option('display.max_columns', None)
            self.legacy_df = pd.read_sql(sql_query, datacon.engine)
            print(self.legacy_df.head())
            return self.legacy_df
            #print(self.legacy_df.tail())

    def retrieve_pdf_data(self, link_pdf):
        pd.set_option('display.max_columns', None)
        #user_card_df = tabula.read_pdf(link_pdf,pages = "all", stream = True)
        self.combined_df = pd.concat(tb.read_pdf(link_pdf, pages = "all"))
        # combined_df = pd.DataFrame()
        # for page in range(len(num_pages_all)):
        #     combined_df = pd.concat([combined_df, num_pages_all[page]], ignore_index= True)
        #     #sample_data = page.head()
        #print(self.combined_df.columns)
        #print(self.combined_df.info())
        return self.combined_df

    def list_number_of_stores(self, no_stores_endpoint , store_header : dict):
        response = requests.get(no_stores_endpoint, headers=store_header)
        num_stores = response.json()['number_stores']
        print(num_stores)
        return num_stores

    def retrieve_stores_data(self, store_retrieve, store_header:dict, store_numbers):
        all_store_data = []
        for store_number in range(0, store_numbers) :
            #store_url = f"{store_retrieve}{store_number}"
            response = requests.get(f"{store_retrieve}{store_number}", headers = store_header)

            if response.status_code == 200:
                store_data = response.json()
                all_store_data.append(store_data)
            else:
                print(f"API request for store {store_number} failed with status code:", response.status_code)

        pd.set_option('display.max_columns', None)
        self.store_df = pd.DataFrame(all_store_data)
        #print(self.store_df.head())
        return self.store_df

    def extract_from_s3(self, address_uri):  
        uri_parts = address_uri.split('/')
        bucket_part = uri_parts[2]
        print('bucket_part', bucket_part)
        uri_id_part = uri_parts[3]
        print('uri_id_part',uri_id_part)
        local_add = '/Users/chait/OneDrive/Desktop/Aicore_project/Multinational_Retail_Data_Centralisation/AWS_S3_Object/products.csv'
        s3 = boto3.client('s3')
        s3.download_file(bucket_part,uri_id_part,local_add)
        #s3.close()
        pd.set_option('display.max_columns', None)
        products_df = pd.read_csv('AWS_S3_Object/products.csv')
        #print(products_df)
        return products_df
    
    def date_event_ext(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            date_df = pd.read_json(response.text)
            print(date_df.head())
            return date_df
        else:
            print('Failed to download the json file having status code= ', response.status_code)

        

    
   
        
        

        #table = DatabaseConnector.list_db_tables()
# if __name__ == "__main__":
#     #dataex = DataExtractor()
#     #print('DataExtraction Class: ',dataex.__str__())
#     #datacon = DatabaseConnector()
#     #print('DataExtraction Class: ',datacon.__str__())
#     table_ext = DataExtractor()
#     pdFrame=dataex.read_rds_table(datacon,"legacy_users")
#     dataClean = DataCleaning()
#     cleanPdFrame= dataClean.clean_user_data(pdFrame)
#     datacon.upload_to_db(cleanPdFrame,"dim_users")

