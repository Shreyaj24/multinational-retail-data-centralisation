import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd
import urllib.parse

#Class definition
class DatabaseConnector():
    '''
    This class is for connecting and uploading data to the database.
    '''
    #class constructor
    def __init__(self):
        #attributes
        self.engine = None
        self.table_list = []
        
    #methods
    def read_db_creds (self):
        '''
        This method extracts the credentials from yaml file.
        '''
        with open('db_creds.yaml', 'r') as yaml_cred_file:
            cred_data = yaml.safe_load(yaml_cred_file)
        return cred_data
    
    def init_db_engine(self):
        '''
        This method creates the engine by extracting details from the above method.
        '''
        cred_data = self.read_db_creds()
        db_conn = f"{'postgresql'}://{cred_data['RDS_USER']}:{cred_data['RDS_PASSWORD']}@{cred_data['RDS_HOST']}:{cred_data['RDS_PORT']}/{cred_data['RDS_DATABASE']}"
        self.engine = create_engine(db_conn)
    
    def list_db_tables (self):
        '''
        This method gets the name of table stored in the RDS. 
        '''
        self.init_db_engine()
        self.engine.connect()
        inspector = inspect(self.engine)
        table_list = inspector.get_table_names()
        return table_list

    def upload_to_db(self, dataframe : pd.DataFrame , table_name_up : str) :
        '''
        This method uploads the data (pd dataframe) into the postgres SQL DB
        '''
        DATABASE_TYPE = 'postgresql'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'Postgre@2023'
        DATABASE = 'sales_data'
        PORT = 5432
        quoted_password = urllib.parse.quote_plus(PASSWORD)

        up_engine = create_engine(f"{DATABASE_TYPE}://{USER}:{quoted_password}@{HOST}:{PORT}/{DATABASE}")
        dataframe.to_sql(table_name_up, up_engine, if_exists= 'replace')    
