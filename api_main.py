from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector


if __name__ == "__main__":
    dataex = DataExtractor()
    dataclean = DataCleaning()
    datacon = DatabaseConnector()
    end_point = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    header_store = {"x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"} 
    no_of_stores=dataex.list_number_of_stores(end_point, header_store)
    retrieve_end_point  = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
    header_store = {"x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    dataex.retrieve_stores_data(retrieve_end_point, header_store, no_of_stores)
    clean_store_data = dataclean.clean_store_data(dataex.store_df)
    datacon.upload_to_db(clean_store_data, "dim_store_details")
