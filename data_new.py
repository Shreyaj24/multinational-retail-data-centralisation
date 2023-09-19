#from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd
#import database_utils
#Class definition
class DataExtractorNew():
    '''
    This class works as a utility class, in it we create methods that help extract data from different data sources.
    The methods contained extract data from a particular data source, these sources include CSV files, an API and an S3 bucket.
    '''
    #class constructor
    #def __init__(self):
        #attributes
    #method
    

dataex = DataExtractor()
print('DataNew : ',dataex.__str__())
datacon = DatabaseConnector()
print('DataNew : ',datacon.__str__())        
data_cl = DataCleaning()
print('DataNew : ',data_cl.__str__())
data_cl.clean_user_data()