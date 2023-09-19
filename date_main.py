from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

if __name__ == "__main__":
    dataex = DataExtractor()
    dataclean = DataCleaning()
    datacon = DatabaseConnector()
    url = ' https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    date_df = dataex.date_event_ext(url)
    clean_date_df = dataclean.clean_date_data(date_df)
    # clean_products_df = dataclean.convert_product_weights(products_df)
    # com_clean_products_df = dataclean.clean_products_data(clean_products_df)
    datacon.upload_to_db(clean_date_df, "dim_date_times")
