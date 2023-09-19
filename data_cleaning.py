#from data_extraction import DataExtractor
#from database_utils import DatabaseConnector
import pandas as pd
import sqlalchemy 
from sqlalchemy import create_engine
import urllib.parse
import re


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
        #print(dataex.legacy_df['country'].unique())
        inv_list = ['NULL', 'I7G4DMDZOZ', 'AJ1ENKS3QL','XGI7FM0VBJ','S0E37H52ON', 'XN9NGL5C0B', '50KUU3PQUF',
                    'EWE3U0DZIV', 'GMRBOMI0O1', 'YOTSVPRBQ7', '5EFAFD0JLI', 'PNRMPSYR1J', 'RQRB7RMTAD', '3518UD5CE8',
                    '7ZNO5EBALT', 'T4WBZSW0XI']
        inv_data_drop = legacy_df['country'].isin(inv_list)
        legacy_df = legacy_df.loc[~(inv_data_drop)]    
        legacy_df['phone_number'] = legacy_df['phone_number'].apply(lambda num: num.replace('x','-'))
        legacy_df['phone_number'] = legacy_df['phone_number'].apply(lambda num: num.replace('.','-'))
        legacy_df['address'] = legacy_df['address'].apply(lambda adr: adr.replace('\n',','))
        legacy_df['country'] = legacy_df['country'].astype(str)
        #print(type(dataex.legacy_df['join_date'][0]))
        legacy_df['join_date'] = pd.to_datetime(legacy_df['join_date'], format='%Y-%m-%d', errors='coerce')
        legacy_df['date_of_birth'] = pd.to_datetime(legacy_df['date_of_birth'], format='%Y-%m-%d', errors='coerce')
        print(legacy_df.dtypes)
        print(legacy_df['join_date'].unique())
        print(legacy_df)
        return legacy_df
    
    def clean_card_data(self, combined_df: pd.DataFrame):
        #print(combined_df['card_provider'].unique())
        junk_list = ['NULL', 'NB71VBAHJE', 'WJVMUO4QX6','JRPRLPIBZ2','TS8A81WFXV', 'JCQMU8FN85', '5CJH7ABGDR', 'DE488ORDXY',
                    'OGJTXI6X1H', '1M38DYQTZV', 'DLWF2HANZF', 'XGZBYBYGUW', 'UA07L7EILH',
                    'BU9U947ZGV', '5MFWFBZRM9']
        #subset_null = combined_df[combined_df['card_provider'].isin(junk_list)].count()
        junk_con = combined_df['card_provider'].isin(junk_list)
        combined_df = combined_df.loc[~(junk_con)]
        #print(combined_df[combined_df['card_provider'] == "VISA 13 digit"]["card_number"].nunique())
        combined_df['date_payment_confirmed'] = pd.to_datetime(combined_df['date_payment_confirmed'],format='%Y-%m-%d', errors='coerce')
        print(combined_df['date_payment_confirmed'][0])
        print(combined_df.head())
        return combined_df

    def clean_store_data(self, store_df: pd.DataFrame):
        print(store_df.columns)
        #print(store_df['country_code'].unique())
        #print(store_df['lat'].isnull().sum())
        store_df = store_df.drop('lat', axis =1)
        store_df = store_df.loc[~(store_df['latitude'].isnull())]
        # store_junk_lat = ['N/A', None]
        # subset_store_lat = store_df.loc[store_df['lat'].isin(store_junk_lat)]
        
        store_junk = ['NULL','QMAVR5H3LD', 'LU3E036ZD9',
                      '5586JCLARW', 'GFJQ2AAEQ8', 'SLQBD982C0', 'XQ953VS0FG', '1WZB1TE1HL']
        #subset_store_junk = store_df.loc[store_df['continent'].isin(store_junk)]
        store_df = store_df.loc[~(store_df['continent'].isin(store_junk))]
        print(store_df['country_code'].unique())
        print(store_df['continent'].unique())
        #subset_store_lat = store_df.loc[store_df['lat'] == 'NULL']
        #print(subset_store_lat)
        store_df['address'] = store_df['address'].apply(lambda adr: adr.replace('\n',','))
        store_df['opening_date'] = pd.to_datetime(store_df['opening_date'], format='%Y-%m-%d', errors='coerce')
        #store_df['country'] = store_df['country_code'].astype(str)
        store_df['continent'] = store_df['continent'].astype(str)
        #store_df['staff_numbers'] = store_df['staff_numbers'].astype(int)
        store_df['latitude'] = store_df['latitude'].astype(float)
        store_df['longitude'] = store_df['longitude'].astype(float)
        print(store_df.loc[store_df['continent'] == 'eeAmerica'])
        print(store_df.head())
        print(store_df.info())
        return store_df
        #print(subset_store_junk)
        #print(store_df.loc[store_df['latitude'] == None].count())

    def convert_product_weights(self, products_df : pd.DataFrame):
        print(products_df.info())
        print(products_df.head())
        products_df = products_df.dropna()
        val_prd = ['Still_avaliable', 'Removed']
        val_prd_cnd = products_df['removed'].isin(val_prd)
        products_df = products_df.loc[(val_prd_cnd)]
        print(products_df['removed'].unique())
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
        print(products_df) 
        print(products_df['weight'].unique())  
        return products_df
    
    
    def clean_products_data(self, products_df: pd.DataFrame):
        #print(type(products_df['weight'][0]))
        #products_df = products_df[products_df['weight'].apply(lambda wg: type(wg)== "float")]
        #pd.options.display.float_format = '{:.2f}'.format
        def reg_ex_mch(value):
            match =  re.match(r'^\d+(\.\d+)?\s*$',str(value))
            weig =  float(match.group()) if match is not None else None
            return weig
        
        products_df['weight'] = products_df['weight'].apply(reg_ex_mch)
        products_df = products_df.dropna(subset = ['weight'])
        products_df['weight'] = products_df['weight'].apply(lambda x: '{:.2f}'.format(x))
        products_df['date_added'] = pd.to_datetime(products_df['date_added'], format='%Y-%m-%d', errors='coerce')
        return products_df
    
    def clean_orders_data(self, orders_df:pd.DataFrame):
        print(orders_df.head())
        print(orders_df.columns)
        drop_ord_df_col = ['first_name','last_name','1']
        orders_df = orders_df.drop(columns=drop_ord_df_col)
        orders_df = orders_df.rename(columns={'level_0':'level_ord'})
        print(orders_df.columns)
        print(orders_df.head())
        return orders_df
    
    def clean_date_data(self, date_df: pd.DataFrame):
        #print(date_df.head())
        print(date_df.dtypes)
        print(date_df.isnull().sum())
        # print(date_df['time_period'].unique())
        # print(date_df['year'].unique())
        val_time_period = ['Evening', 'Morning', 'Midday', 'Late_Hours']
        val_time_period_cnd = date_df['time_period'].isin(val_time_period)
        date_df = date_df.loc[(val_time_period_cnd)]
        print(date_df['time_period'].unique())
        print(date_df['year'].unique())
        print(date_df.head())
        return date_df



        



# dataex = DataExtractor()
# datacon = DatabaseConnector()        
# data_cl = DataCleaning()
# data_cl.clean_user_data(dataex,datacon)
# datacon.upload_to_db(dataex.legacy_df, "dim_users")