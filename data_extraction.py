from database_utils import DatabaseConnector
import pandas as pd
import tabula as tb
import boto3
import requests

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

    #method
    def read_rds_table(self, datacon : DatabaseConnector, table_name : str):
        '''
        This method reads the table from RDS and extracts the data
        of a particular table(legacy users data)
        '''
        list_tables = datacon.list_db_tables()
        if table_name in list_tables:
            sql_query = f'SELECT * from {table_name}'
            pd.set_option('display.max_columns', None)
            self.legacy_df = pd.read_sql(sql_query, datacon.engine)
            return self.legacy_df

    def retrieve_pdf_data(self, link_pdf):
        '''This method retrieves the data stored in pdf using tabula-py.'''
        pd.set_option('display.max_columns', None)
        self.combined_df = pd.concat(tb.read_pdf(link_pdf, pages = "all"))
        return self.combined_df

    def list_number_of_stores(self, no_stores_endpoint , store_header : dict):
        '''
        This method list the number os stores from API.
        '''
        response = requests.get(no_stores_endpoint, headers=store_header)
        num_stores = response.json()['number_stores']
        return num_stores

    def retrieve_stores_data(self, store_retrieve, store_header:dict, store_numbers):
        '''
        This method retreives the store data from API provided 
        and using the store count derived from above method
        '''
        all_store_data = []
        for store_number in range(0, store_numbers) :
            response = requests.get(f"{store_retrieve}{store_number}", headers = store_header)

            if response.status_code == 200:
                store_data = response.json()
                all_store_data.append(store_data)
            else:
                print(f"API request for store {store_number} failed with status code:", response.status_code)

        pd.set_option('display.max_columns', None)
        self.store_df = pd.DataFrame(all_store_data)
        return self.store_df

    def extract_from_s3(self, address_uri):  
        '''
        This method extracts/downloads data from S3 bucket using boto3 library
        '''
        uri_parts = address_uri.split('/')
        bucket_part = uri_parts[2]
        uri_id_part = uri_parts[3]
        local_add = '/Users/chait/OneDrive/Desktop/Aicore_project/multinational_Aicore_project/multinational-retail-data-centralisation/AWS_S3_Object/products.csv'
        s3 = boto3.client('s3')
        s3.download_file(bucket_part,uri_id_part,local_add)
        pd.set_option('display.max_columns', None)
        products_df = pd.read_csv('AWS_S3_Object/products.csv')
        return products_df
    
    def date_event_ext(self, url):
        '''
        This method extracts json file stored in S3 bucket having a link.
        '''
        response = requests.get(url)
        if response.status_code == 200:
            date_df = pd.read_json(response.text)
            return date_df
        else:
            print('Failed to download the json file having status code= ', response.status_code)
