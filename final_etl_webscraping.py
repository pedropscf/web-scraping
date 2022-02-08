import requests
import math
import re
import pandas as pd
import numpy as np
import os
import logging
import sqlite3

from bs4 import BeautifulSoup
from datetime import datetime
from sqlalchemy import create_engine
    

#-----------------------------------------------------------------------------------------------------------------------
# HM DATA COLLECTION FUNCTION
#-----------------------------------------------------------------------------------------------------------------------

def data_collection(url, headers):
    # Parameters
    #headers={'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

    # URL
    #url = 'https://www2.hm.com/en_us/men/products/jeans.html'

    # Request to URL
    page = requests.get(url, headers=headers)

    # Request response check
    logger.info('The page variable will contain the response from the HTML page, where 200 means it worked. The response is: %s', page)

    # Beautiful soup object
    soup = BeautifulSoup(page.text, 'html.parser')

    # ===================== Product data ===================================#

    # Products available from the showroom
    products = soup.find('ul', class_='products-listing small')
    product_list = products.find_all('article', class_='hm-product-item')

    #id
    product_id = [p.get('data-articlecode') for p in product_list]
    #category
    product_cat = [p.get('data-category') for p in product_list]
    #product name
    name = products.find_all('a', class_='link')
    product_name = [p.get_text() for p in name]
    #price
    price = products.find_all('span', class_='price regular')
    product_price = [p.get_text() for p in price]

    # Creating the dataframe with the products in it
    data = pd.DataFrame([product_name, product_id, product_cat, product_price]).T
    data.columns = ['product_name', 'product_id', 'product_cat', 'product_price']
    
    return data

#-----------------------------------------------------------------------------------------------------------------------
# HM DATA COLLECTION BY PRODUCT FUNCTION
#-----------------------------------------------------------------------------------------------------------------------

def data_collection_by_product(data, headers):

    # Parameter
    #headers={'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5), AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

    # Empty dataframe
    df_compositions = pd.DataFrame()

    # Unique columns for all products
    aux = []

    # Standard format of composition informations
    cols = ['Fit', 'Composition', 'Art. No.', 'Product safety', 'Size']
    df_pattern = pd.DataFrame(columns=cols)

    for i in range( len(data)):

        #API Request
        url = 'https://www2.hm.com/en_us/productpage.' + data.loc[i,'product_id'] + '.html'
        page = requests.get( url, headers=headers)


        #Creating the BeautifulSoup object
        soup = BeautifulSoup( page.text, 'html.parser')


        #========================================= COLOR =============================================#
        # Starting to search the color name
        product_list = soup.find_all('a', class_='filter-option miniature') + soup.find_all('a', class_='filter-option miniature active')
        colors = [p.get('data-color') for p in product_list]

        # Product ID
        product_id = [p.get('data-articlecode') for p in product_list]

        # Creating dataframe for product with its color
        df_color = pd.DataFrame( [product_id, colors] ).T
        df_color.columns = ['product_id', 'color']

        # Generating style ID + color ID
        #df_color['style_id'] = df_color['product_id'].apply( lambda x: x[:-3])
        #df_color['color_id'] = df_color['product_id'].apply( lambda x: x[-3:])

        for j in range(len(df_color)):
            #API Request
            url = 'https://www2.hm.com/en_us/productpage.' + df_color.loc[j,'product_id'] + '.html'
            page = requests.get( url, headers=headers)


            #Creating the BeautifulSoup object
            soup = BeautifulSoup( page.text, 'html.parser')

            #========================================= NAME =============================================#
            product_name_color = soup.find_all('h1', class_='primary product-item-headline')
            product_name_color = product_name_color[0].get_text()
            product_name_color = re.findall(r'\w+\ ?\w+\ ?\w+', product_name_color)[0]

            #========================================= PRICE =============================================#
            product_price = soup.find_all('span', class_='price-value') #('div', class_='primary-row product-item-price')
            product_price = product_price[0].get_text()
            product_price = re.findall( r'\S\d+\.?\d+', product_price )[0]

            logger.debug('URL of the product: %s \n The name of the product is: %s, and its price is: %s' % (url, product_name_color, product_price))
            #print("URL of the product: {} \n The name of the product is: {}, and its price is: {}".format(url, product_name_color, product_price))

            #========================================= COMPOSITION==========================================#
            # Starting to search product composition
            product_comp = soup.find_all('div', 'pdp-description-list-item')
            composition = [list(filter(None, p.get_text().split('\n') )) for p in product_comp]

            # Creating dataframe
            df_composition = pd.DataFrame(composition).T
            df_composition.columns = df_composition.iloc[0]

            # Deleting the first row of the dataframe
            df_composition = df_composition.iloc[1:].fillna(method='ffill')

            # Removing pocket lining, shell and lining
            df_composition['Composition'] = df_composition['Composition'].str.replace('Pocket lining: ', '', regex=True)
            df_composition['Composition'] = df_composition['Composition'].str.replace('Shell: ', '', regex=True)
            df_composition['Composition'] = df_composition['Composition'].str.replace('Lining: ', '', regex=True)
            df_composition['Composition'] = df_composition['Composition'].str.replace('Pocket: ', '', regex=True)


            # Guaranteeing the same number of columns
            df_composition = pd.concat([df_pattern, df_composition], axis=0 )

            # Generating style ID + color ID
            df_composition['style_id'] = df_composition['Art. No.'].apply( lambda x: x[:-3])
            df_composition['color_id'] = df_composition['Art. No.'].apply( lambda x: x[-3:])

            # Adding the name and price of the products
            df_composition['product_name'] = product_name_color
            df_composition['product_price'] = product_price

            # If a new column appears, it will be attached to the auxiliar list
            aux = aux + df_composition.columns.tolist()

	    # Dropping new columns
            df_composition.drop(columns='More sustainable materials', axis='columns', inplace=True, errors='ignore')


            # Renaming the columns before the concatenation
            df_composition.columns = ['fit', 'composition', 'product_id','product_safety', 'size', 'style_id', 'color_id', 'product_name', 'product_price']

            # Merging data acquired
            df_composition= pd.merge(df_composition, df_color, how='left', on='product_id')

            # Products with all informations
            df_compositions = pd.concat( [df_compositions, df_composition], axis=0)


    # Scrappy datetime
    df_compositions['scrapy_time'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    
    return df_compositions

#-----------------------------------------------------------------------------------------------------------------------
# HM DATA COLLECTION FUNCTION
#-----------------------------------------------------------------------------------------------------------------------

def data_cleaning(data):
    # Loading raw data from previous step or from a CSV
    #data = df_compositions.copy() # data = pd.read_csv('datasets/data_raw_hm.csv')
    logger.info('Before cleaning, the dataset has the dimensions of: %s, with %s unique products' % (data.shape, len(data['product_id'].unique())))

    # product_id
    data = data.dropna(subset=['product_id'])
    #data['product_id'] = data['product_id'].astype(int)

    # product_category

    # product_name
    data['product_name'] = data['product_name'].apply(lambda x: x.replace(' ', '_').lower() if pd.notnull(x) else x)

    # Removing $ from the product price
    data['product_price'] = data['product_price'].apply(lambda x: x.replace('$', '') if pd.notnull(x) else x )
    # product_price
    data['product_price'] = data['product_price'].astype(float)

    # scrapy datetime
    data['scrapy_time'] = pd.to_datetime( data['scrapy_time'], format='%Y-%m-%d %H:%M:%S' )

    # style_id
    #data['style_id'] = data['style_id'].astype(int)

    # color_id
    #data['color_id'] = data['color_id'].astype(int)

    # color
    data['color'] = data['color'].apply(lambda x: x.replace(' ', '_').replace('/','_').lower() if pd.notnull(x) else x)

    # fit
    data['fit'] = data['fit'].apply(lambda x: x.replace(' ', '_').replace('/','_').lower() if pd.notnull(x) else x)


    # size - from size, we will extract 2 features with regex
    data['size_number'] = data['size'].apply(lambda x: re.search('\d{3}cm', x).group(0)  if pd.notnull(x) else x)
    data['size_number'] = data['size_number'].apply(lambda x: x.replace('cm', '') if pd.notnull(x) else x)

    data['size_model'] = data['size'].str.extract( '(\d+/\\d+)' )

    # removing duplicates
    #data = data.drop_duplicates(subset=['product_name', 'product_id', 'product_cat', 'product_price',
    #       'scrapy_time', 'style_id', 'color_id', 'color', 'Fit'], keep='last')

    # reset index
    #data = data.reset_index(drop=True)

    # ==================================== COMPOSITION =======================================
    # break composition
    df_comp = data['composition'].str.split(',', expand=True).reset_index(drop=True)
    df_ref = pd.DataFrame(index=np.arange( len(data)), columns=['cotton','polyester','elastane', 'elasterell','other'])

    # looking for cotton composition in df_comp -------------------- Cotton ---------------
    df_cotton_0 = df_comp.loc[df_comp[0].str.contains('Cotton', na=True), 0]
    df_cotton_0.name = 'cotton'
    df_cotton_1 = df_comp.loc[df_comp[1].str.contains('Cotton', na=True), 1]
    df_cotton_1.name = 'cotton'
    # combining cotton results
    df_cotton = df_cotton_0.combine_first(df_cotton_1)
    df_ref = pd.concat([df_ref,df_cotton], axis=1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    # looking for polyester composition in df_comp -------------------- Polyester ---------------
    df_polyester_0 = df_comp.loc[df_comp[0].str.contains('Polyester', na=True), 0]
    df_polyester_0.name = 'polyester'
    df_polyester_1 = df_comp.loc[df_comp[1].str.contains('Polyester', na=True), 1]
    df_polyester_1.name = 'polyester'
    # combining polyester results
    df_polyester = df_polyester_0.combine_first(df_polyester_1)
    df_ref = pd.concat([df_ref,df_polyester], axis=1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    # looking for elastane composition in df_comp -------------------- Elastane ---------------
    df_elastane_1 = df_comp.loc[df_comp[0].str.contains('Spandex', na=True), 0]
    df_elastane_1.name = 'spandex'
    df_elastane_2 = df_comp.loc[df_comp[1].str.contains('Spandex', na=True), 1]
    df_elastane_2.name = 'spandex'
    df_elastane_3 = df_comp.loc[df_comp[2].str.contains('Spandex', na=True), 2]
    df_elastane_3.name = 'spandex'
    # combining elastane results
    df_elastane_interm = df_elastane_1.combine_first(df_elastane_2)
    df_elastane = df_elastane_interm.combine_first(df_elastane_3)
    df_ref = pd.concat([df_ref,df_elastane], axis=1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    # looking for elasterell composition in df_comp -------------------- Elasterell ---------------
    df_elasterell_1 = df_comp.loc[df_comp[1].str.contains('Elasterell', na=True), 1]
    df_elasterell_1.name = 'elasterell'
    # combining elasterell results
    df_ref = pd.concat([df_ref,df_elasterell_1], axis=1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    # joining with the product_id
    df_aux = pd.concat([data['product_id'].reset_index(drop=True), df_ref], axis=1)

    # format composition data
    df_aux['cotton'] = df_aux['cotton'].apply(lambda x: int(re.search('\d+', x).group(0)) if pd.notnull(x) else 0)
    df_aux['polyester'] = df_aux['polyester'].apply(lambda x: int(re.search('\d+', x).group(0)) if pd.notnull(x) else 0)
    df_aux['spandex'] = df_aux['spandex'].apply(lambda x: int(re.search('\d+', x).group(0)) if pd.notnull(x) else 0)
    df_aux['elasterell'] = df_aux['elasterell'].apply(lambda x: int(re.search('\d+', x).group(0)) if pd.notnull(x) else 0)
    #df_aux['other'] = df_aux['other'].apply(lambda x: int(re.search('\d+', x).group(0)) if pd.notnull(x) else 0)

    # final join
    df_aux = df_aux.groupby('product_id').max().reset_index().fillna(0)
    data = pd.merge(data, df_aux, on='product_id', how='left')

    # droping unnecessary data
    data = data.drop(columns=['size', 'product_safety', 'composition', 'other'])

    # Drop duplicates
    data = data.drop_duplicates()

    # Checking dataframe size after cleaning 
    logger.info("The dimensions of the data frame after cleaning is: %s", data.shape)
    
    return data

#-----------------------------------------------------------------------------------------------------------------------
# HM DATA COLLECTION FUNCTION
#-----------------------------------------------------------------------------------------------------------------------

def data_insertion(data):

    data_insert = data[[
        'product_id',
        'style_id',
        'color_id',
        'product_name',
        'color',
        'fit',
        'product_price',
        'size_number',
        'size_model',
        'cotton',
        'polyester',
        'spandex',
        'elasterell',
        'scrapy_time'
    ]]

    #query_showroom_schema = '''
    #    CREATE TABLE vitrine(
    #    product_id     TEXT,
    #    style_id       TEXT,
    #    color_id       TEXT,
    #    product_name   TEXT,
    #    color          TEXT,
    #    fit            TEXT,
    #    product_price  REAL,
    #    size_number    TEXT,
    #    size_model     TEXT,
    #    cotton         REAL,
    #    polyester      REAL,
    #    spandex       REAL,
    #    elasterell     REAL,
    #    scrapy_time    TEXT    
    #    )
    #'''

    # Droping the table - NOT TO USE
    #query_drop = '''
    #    DROP TABLE vitrine
    #'''

    # Create table
    #conn = sqlite3.connect('database_hm.sqlite')
    #cursor = conn.execute(query_showroom_schema)
    #conn.commit()

    # Creating connection to database
    conn = create_engine('sqlite:///database_hm.sqlite', echo=False)

    # Inserting data
    data_insert.to_sql('vitrine', con=conn, if_exists='append', index=False)

    # Checking the table
    #query = '''
    #    SELECT * FROM vitrine
    #'''

    #df = pd.read_sql_query(query, conn)
    #df.head()

#-----------------------------------------------------------------------------------------------------------------------
# HM DATA COLLECTION FUNCTION
#-----------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':

    # Logging data
    path = '/home/pedro/Documents/repos/web-scraping/'

    if not os.path.exists(path + 'Logs'):
        os.makedirs(path + 'Logs')
        
    logging.basicConfig(
        filename= path + 'Logs/webscraping_hm' + str(datetime.now().strftime('%Y-%m-%d')) + '.txt',
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M%S',
        level=logging.INFO
        )
    logger = logging.getLogger('webscraping_hm')


    # Parameters and constants
    url = 'https://www2.hm.com/en_us/men/products/jeans.html?sort=stock&image-size=small&image=model&offset=0&page-size=999'
    headers={'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    logger.info('Starting webscraping at: %s', url)

    # Data collection
    data = data_collection(url, headers)
    logger.info('Data collection done')

    # Data collection by product
    data_product = data_collection_by_product(data, headers)
    logger.info('Data collection by product done')

    # Data cleaning
    data_clean = data_cleaning(data_product)
    logger.info('Data cleaning done')

    # Data insertion
    data_insertion(data_clean)
    logger.info('Data insertion done')
