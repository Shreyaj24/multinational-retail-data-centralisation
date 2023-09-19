from data_extraction import DataExtractor 
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning

if __name__ == "__main__":    
    datacon = DatabaseConnector()
    dataext = DataExtractor()
    dataext.read_rds_table(datacon,"legacy_users")
    #print('DataExtraction Class: ',dataex.__str__())
    #datacon = DatabaseConnector()
    #print('DataExtraction Class: ',datacon.__str__())
    datacleaner= DataCleaning()
    data_clean_pd = datacleaner.clean_user_data(dataext.legacy_df)
    datacon.upload_to_db(data_clean_pd, "dim_users2")
    #dataex.read_rds_table(datacon,"legacy_users")