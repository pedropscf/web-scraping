# E-Commerce insights trough ETL developing and web scrapping

The All Star Jeans is a brand new company that will begin to its e-commerce by selling only jeans for men. However, the company wants to open a new business in selling jeans for men in the United States of America. The company does not know the trends in that market nor about the precification of the products.

Because of this, the company hired us to inform them about pricing, products trends and characteristics of the products in order to be able to compete with other retailers. The main business questions to be answered are:

- **How many different models of products are available at a competitor retailer?**
- **How many different colors are available at other e-commerces?**
- **What are the composition of the material of the clothes?**
- **At what prices the jeans are being sold?**

## Solution

The solution develop to answer those business questions was to develop an ETL to extract data available in the web of one of the biggest retailers of the US, the H&M. We used the Beautiful Soup package and python to develop an ETL, that is able to access the e-commerce website of H&M, look for the desired products (jeans for men) and extract information about pricing, colors, composition and variety of products available. Also, we used Cron to schedule our ETL job to frequently look at their page and insert data into our database. A summarized view of the solution is (the complete version of solution along with all the code can be [seem here](https://github.com/pedropscf/web-scraping/blob/7291eb8f2ade1691423c6c2868ccdf8324445a5f/web-scraping-etl.ipynb)):

## Data collection (Extract)

One of the first step is to access the [showroom with all H&M products](https://www2.hm.com/en_us/men/products/jeans.html?sort=stock&image-size=small&image=model&offset=0&page-size=999) and with the Beautiful Soup object, try to extract an identifier of all products. This is a iterative and incremental process that is done by inspecting the HTML of the page and trying to look where this information is stored.

## Data collection by product (Extract)

With the identifier of each product we [can access each of the products pages available](https://www2.hm.com/en_us/productpage.0979945002.html). At this point, another iterative and incremental process of inspecting the HTML is started, in order for us to extract information about pricing, colors available and composition of the jeans. It is done by searching for a color identifier whithin the product page. For the previous link, we can access 3 different colors: [light denim blue](https://www2.hm.com/en_us/productpage.0979945001.html), [denim blue](https://www2.hm.com/en_us/productpage.0979945002.html) and [black](https://www2.hm.com/en_us/productpage.0979945003.html).

## Data cleaning (Transform)

After a brief look at the dataframe created from all scrapping of products and colors, we can see that we have some columns with null values, duplicated values and a column with nested characteristics (the composition column are lists). We have to clean the data in order to prepare them to load.

## Data insertion (Load)

With all the data acquired and cleaned, the next step is to store it in a database. The main goal of the database is for us to keep track at the H&M pricing of products, variety, colors and material composition. Thus, making it possible for us to answer the business questions.

The database created is a relational database based on SQLite. The main features stored are features like: scrap datetime, identifiers of products and colors, price, product name, color name and product composition.

## Exploratory data analysis

With data from the last week, extracted 4 times along the period, we can answer the business questions:


- **How many different models of products are available at a competitor retailer?** There are a total of
- **How many different colors are available at other e-commerces?** Each product ID has an average of X colors, with the main colors being
- **What are the composition of the material of the clothes?** The main materials used at the clothes are cotton, polyester and spandex. The average product contains
- **At what prices the jeans are being sold?** There are different pricess, but we can see that cheapest is $19.99 and the most expensive is $39.99

**Observation:** if we keep extracting and analyzing the data of H&M we can keep up with its trends, prices and product quality. So, it is expected that the result and insights from the analysis change along the time.

## Technologies used:
**Data manipulation and cleaning:** pandas
**Web scrapping:** Beautiful Soup
**Database connection and data insertion:** SQLAlchemy and SQLite
**Job scheduler:** cron
**ETL report and log:** logger

## About me

I am data science enthusiast, learning new applications of Machine Learning, AI and Data Science in general to solve a diverse range of business problems.

## Author

- Pedro Fernandes [@pedropscf](https://www.github.com/pedropscf)
