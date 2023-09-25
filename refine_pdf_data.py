#from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

if __name__ == "__main__":
    dataex = DataExtractor()
    dataclean = DataCleaning()
    datacon = DatabaseConnector()
    dataex.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
    clean_card_det = dataclean.clean_card_data(dataex.combined_df)
    datacon.upload_to_db(clean_card_det, "dim_card_details")