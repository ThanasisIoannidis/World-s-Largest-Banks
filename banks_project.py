import requests
import pandas as pd
import numpy as np
import datetime as dt
import sqlite3
from bs4 import BeautifulSoup


url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attr = ["Name","MC_USD_Billion"]
csv_output_path = "./Largest_banks_data.csv"
db_name = "Banks.db"
table_name = "Largest_banks"
log_file_path = "./code_log.txt"

query_smt = """ SELECT * FROM Largest_banks """
query_smt2 = """ SELECT AVG(MC_GBP_Billions) FROM Largest_banks """
query_smt3 = """ SELECT Name from Largest_banks LIMIT 5 """

exchange_rate = pd.read_csv('exchange_rate.csv')


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%m-%d-%H:%M:%S'
    now = dt.datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./etl_project_log.txt", 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')



def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, "html.parser")
    df = pd.DataFrame(columns=table_attr)

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')

        if len(col) >0:
            data_dict = {"Name":col[1].get_text(strip = True),
                        "MC_USD_Billion": col[2].get_text(strip = True)}
            df1 = pd.DataFrame(data_dict, index = [0])
            df = pd.concat([df, df1], ignore_index= True)
     
    return df

# extract(url, table_attr)
# extracted_df = extract(url, table_attr)
# print(extracted_df)



def transform(df, exchange_rate):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies.

    Columns to add:
        MC_GBP_Billion, MC_EUR_Billion and MC_INR_Billion

    Table Attributes (final):
        Name, MC_USD_Billion, MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion
    '''
    df = df.astype({"Name":"string","MC_USD_Billion":"float"})

    for _, row in exchange_rate.iterrows():
      df[f'MC_{row["Currency"]}_Billions'] = np.round(df["MC_USD_Billion"] * row['Rate'], 2) 

    return df

# transformed_df = transform(extracted_df, exchange_rate)
# transformed_df['MC_EUR_Billions'][4]
# print(transformed_df)


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    if len(df) > 0:
        df.to_csv(output_path)
    else:
        print("Dataframe is empty!. The file wasn't created")

#load_to_csv(transformed_df, output_csv_path)


def load_to_db(df, sql_connection, table_name):
    '''This function saves the final dataframe as a database table
       with the provided name. Function returns nothing.'''
    #sql_connection = db_name
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)



def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)



# Error handling
sql_connection = sqlite3.connect('World_Economies.db')
try:
    log_progress('Preliminaries complete. Initiating ETL process')

    # Data Extraction
    try:
        df_extracted = extract(url, table_attr)
        log_progress('Data extraction complete. Initiating Transformation process')
    except Exception as e:
        log_progress(f'Error during data extraction: {str(e)}')
        raise

    # Data Transformation
    try:
        df_transformed = transform(df_extracted, exchange_rate)
        log_progress('Data transformation complete. Initiating loading process')
    except Exception as e:
        log_progress(f'Error during data transformation: {str(e)}')
        raise

    # Loading data to csv
    try:
        load_to_csv(df_transformed, csv_output_path)
        log_progress('Data saved to CSV file')
    except Exception as e:
        log_progress(f'Error saving data to CSV: {str(e)}')
        raise

    # Connecting to Database
    try:
        sql_connection = sqlite3.connect('World_Economies.db')
        log_progress('SQL Connection initiated.')
    except Exception as e:
        log_progress(f'Error initiating SQL connection: {str(e)}')
        raise

    # Loading data to the Database
    try:
        load_to_db(df_transformed, sql_connection, table_name)
        log_progress('Data loaded to Database as table. Running the query')
    except Exception as e:
        log_progress(f'Error loading data to database: {str(e)}')
        raise

    # Execute queries
    try:
        run_query(query_smt, sql_connection)
        log_progress('Process Complete.')
    except Exception as e:
        log_progress(f'Error running SQL query: {str(e)}')
        raise

    try:
        run_query(query_smt2, sql_connection)
        log_progress('Process Complete.')
    except Exception as e:
        log_progress(f'Error running SQL query: {str(e)}')
        raise

    try:
        run_query(query_smt3, sql_connection)
        log_progress('Process Complete.')
    except Exception as e:
        log_progress(f'Error running SQL query: {str(e)}')
        raise

finally:
    # Cierre seguro de la conexión
    try:
        if sql_connection:
            sql_connection.close()
            log_progress('SQL connection closed.')
    except Exception as e:
        log_progress(f'Error closing SQL connection: {str(e)}')


df_extracted.head()
df_transformed.head()