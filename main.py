from data_extraction import DataExtractor 
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning

if __name__ == "__main__":    
    # defining extraction and cleaning variable objects
    datacon = DatabaseConnector()
    dataext = DataExtractor()
    datacleaner= DataCleaning()

    #uploading users data from RDS DB 
    dataext.read_rds_table(datacon,"legacy_users")
    data_clean_pd = datacleaner.clean_user_data(dataext.legacy_df)
    datacon.upload_to_db(data_clean_pd, "dim_users")

    #uploading card details from PDF file source
    dataext.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
    clean_card_det = datacleaner.clean_card_data(dataext.combined_df)
    datacon.upload_to_db(clean_card_det, "dim_card_details")

    #uploading store details from API response
    end_point = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    header_store = {"x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"} 
    no_of_stores=dataext.list_number_of_stores(end_point, header_store)
    retrieve_end_point  = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
    header_store = {"x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    dataext.retrieve_stores_data(retrieve_end_point, header_store, no_of_stores)
    clean_store_data = datacleaner.clean_store_data(dataext.store_df)
    datacon.upload_to_db(clean_store_data, "dim_store_details")

    #uploading product details from CSV file in an AWS S3 bucket
    uri = 's3://data-handling-public/products.csv'
    products_df = dataext.extract_from_s3(uri)
    clean_products_df = datacleaner.convert_product_weights(products_df)
    com_clean_products_df = datacleaner.clean_products_data(clean_products_df)
    datacon.upload_to_db(com_clean_products_df, "dim_products")

    #uploading orders table
    orders_df = dataext.read_rds_table(datacon,"orders_table")
    clean_ord_df = datacleaner.clean_orders_data(orders_df)
    datacon.upload_to_db(clean_ord_df, "orders_table")

    #uploading date_time_details from JSON file in an AWS S3 bucket
    url = ' https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    date_df = dataext.date_event_ext(url)
    clean_date_df = datacleaner.clean_date_data(date_df)
    datacon.upload_to_db(clean_date_df, "dim_date_times")