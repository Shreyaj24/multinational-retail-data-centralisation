from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

if __name__ == "__main__":
    dataex = DataExtractor()
    dataclean = DataCleaning()
    datacon = DatabaseConnector()
    uri = 's3://data-handling-public/products.csv'
    products_df = dataex.extract_from_s3(uri)
    clean_products_df = dataclean.convert_product_weights(products_df)
    com_clean_products_df = dataclean.clean_products_data(clean_products_df)
    datacon.upload_to_db(com_clean_products_df, "dim_products")


