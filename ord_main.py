from data_extraction import DataExtractor 
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning

if __name__ == "__main__":    
    datacon = DatabaseConnector()
    dataext = DataExtractor()
    dataclean = DataCleaning()
    #dataext.read_rds_table(datacon,"legacy_users")
    #datacon.list_db_tables()
    orders_df = dataext.read_rds_table(datacon,"orders_table")
    clean_ord_df = dataclean.clean_orders_data(orders_df)
    datacon.upload_to_db(clean_ord_df, "orders_table")