import pandas as pd
import sqlalchemy 
from sqlalchemy import create_engine
import urllib.parse
import re
import numpy as np

#Class definition
class DataCleaning():
    '''
    This class is having methods to clean the data from each of the data sources
    '''
    #class constructor
    #def __init__(self):
        #attributes

    #methods
    def clean_user_data(self, legacy_df:pd.DataFrame):
        '''
        This method cleans the user db and returns the pd dataframe. 
        '''
        inv_list = ['NULL', 'I7G4DMDZOZ', 'AJ1ENKS3QL','XGI7FM0VBJ','S0E37H52ON', 'XN9NGL5C0B', '50KUU3PQUF',
                    'EWE3U0DZIV', 'GMRBOMI0O1', 'YOTSVPRBQ7', '5EFAFD0JLI', 'PNRMPSYR1J', 'RQRB7RMTAD', '3518UD5CE8',
                    '7ZNO5EBALT', 'T4WBZSW0XI']
        inv_data_drop = legacy_df['country'].isin(inv_list)
        legacy_df = legacy_df.loc[~(inv_data_drop)]    
        legacy_df['phone_number'] = legacy_df['phone_number'].apply(lambda num: num.replace('x','-'))
        legacy_df['phone_number'] = legacy_df['phone_number'].apply(lambda num: num.replace('.','-'))
        legacy_df['address'] = legacy_df['address'].apply(lambda adr: adr.replace('\n',','))
        legacy_df['country'] = legacy_df['country'].astype(str)
        legacy_df['join_date'] = pd.to_datetime(legacy_df['join_date'], format='%Y-%m-%d', errors='coerce')
        legacy_df['date_of_birth'] = pd.to_datetime(legacy_df['date_of_birth'], format='%Y-%m-%d', errors='coerce')
        return legacy_df
    
    def clean_card_data(self, combined_df: pd.DataFrame):
        '''
        This method cleans the card details and returns the pd Dataframe.
        '''
        val_list_card_prv = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit',
                            'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover', 'VISA 19 digit',
                            'VISA 16 digit', 'VISA 13 digit']
        val_card_con = combined_df['card_provider'].isin(val_list_card_prv)
        combined_df = combined_df.loc[(val_card_con)]
        combined_df['date_payment_confirmed'] = pd.to_datetime(combined_df['date_payment_confirmed'],format='%Y-%m-%d', errors='coerce')
        return combined_df

    def clean_store_data(self, store_df: pd.DataFrame):
        '''
        This method cleans the store details and returns the pd Dataframe
        '''
        store_df = store_df.drop('lat', axis =1)
        val_cont = ['Europe', 'America', 'eeEurope', 'eeAmerica']
        store_df = store_df.loc[(store_df['continent'].isin(val_cont))]
        store_df['address'] = store_df['address'].apply(lambda adr: adr.replace('\n',','))
        store_df['opening_date'] = pd.to_datetime(store_df['opening_date'], format='%Y-%m-%d', errors='coerce')
        store_df['continent'] = store_df['continent'].astype(str)
        return store_df

    def convert_product_weights(self, products_df : pd.DataFrame):
        '''
        This method converts the weight having different units into the kg having decimal values.
        '''
        products_df = products_df.dropna()
        val_prd = ['Still_avaliable', 'Removed']
        val_prd_cnd = products_df['removed'].isin(val_prd)
        products_df = products_df.loc[(val_prd_cnd)]
        val_rem_bool = {'Removed': False, 'Still_avaliable': True}
        products_df['removed'] = products_df['removed'].replace(val_rem_bool)
        def g_ml_to_kg(weight:str):
            if str(weight).endswith('kg'):
                weight = weight.replace('kg','').strip()
                if 'x'.lower() in str(weight):
                    wt_in = str(weight).split('x'.lower())
                    weight = float(wt_in[0]) * float(wt_in[1])
                    return round(weight,2)
                else:
                    return round(float(weight),2)
            elif str(weight).endswith('g'):
                grams = weight.replace('g','').strip()
                if 'x'.lower() in str(grams):
                    wt_in = str(grams).split('x'.lower())
                    grams = float(wt_in[0]) * float(wt_in[1])
                    weight = float(grams)
                    weight = round((weight / 1000),2)
                    return weight
                else:    
                    weight = float(grams)
                    weight = round((weight / 1000),2)
                    return weight
            elif str(weight).endswith('ml'):
                mgram = weight.replace('ml','').strip()
                if 'x'.lower() in str(mgram):
                    wt_in = str(mgram).split('x'.lower())
                    mgram = float(wt_in[0]) * float(wt_in[1])
                    weight = float(mgram)
                    weight = round((weight / 1000),2)
                    return weight
                else:
                    weight = float(mgram)
                    weight = round((weight /1000),2)
                    return weight
            elif str(weight).endswith('oz'):
                ogram = weight.replace('oz','').strip()
                if 'x'.lower() in str(ogram):
                    wt_in = str(ogram).split('x'.lower())
                    ogram = float(wt_in[0]) * float(wt_in[1])
                    weight = float(ogram)
                    weight = round((weight / 1000),2)
                    return weight
                else:
                    weight = float(ogram)
                    weight = round((weight / 1000),2) 
                    return weight 
            else:
                return weight       
        products_df['weight'] = products_df['weight'].apply(g_ml_to_kg)     
        return products_df
    
    
    def clean_products_data(self, products_df: pd.DataFrame):
        '''
        This method cleans the other errors and returns pd Dataframe
        '''
        def reg_ex_mch(value):
            match =  re.match(r'^(\d+(\.\d+)?)\s*(kg|g|oz|ml)?\s*(\.)?$',str(value))
            if match is not None:
                numeric_val  = match.group(1) 
                unit = match.group(3)
                if unit == 'kg':
                    return float(numeric_val)
                elif unit == 'g' or unit == 'oz' or unit == 'ml':
                    return float(numeric_val) /1000
                else:
                    return float(numeric_val)
            else:
                return None
        
        products_df['weight'] = products_df['weight'].apply(reg_ex_mch)
        products_df['weight'] = products_df['weight'].apply(lambda x: '{:.2f}'.format(x))
        products_df['date_added'] = pd.to_datetime(products_df['date_added'], format='%Y-%m-%d', errors='coerce')
        return products_df
    
    def clean_orders_data(self, orders_df:pd.DataFrame):
        '''
        This method cleans the orders data and returns the pd dataframe
        '''
        drop_ord_df_col = ['first_name','last_name','1']
        orders_df = orders_df.drop(columns=drop_ord_df_col)
        orders_df = orders_df.rename(columns={'level_0':'level_ord'})
        return orders_df
    
    def clean_date_data(self, date_df: pd.DataFrame):
        '''
        This method cleans the date events data and returns the pd dataframe
        '''
        val_time_period = ['Evening', 'Morning', 'Midday', 'Late_Hours']
        val_time_period_cnd = date_df['time_period'].isin(val_time_period)
        date_df = date_df.loc[(val_time_period_cnd)]
        return date_df
