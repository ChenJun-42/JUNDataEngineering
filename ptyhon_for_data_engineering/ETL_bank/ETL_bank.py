from datetime import datetime
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3


def log_progress(message):
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open("./code_log.txt", "a") as log_file:
        log_file.write(f"{timestamp} : {message}\n")


def extract(url, table_atrribts_e):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df_e = pd.DataFrame(columns=table_atrribts_e)
    table = data.find("table", {"class": "wikitable sortable mw-collapsible"})
    rows = table.find_all('tr')

    for row in rows:
        if row.find('th'):
            continue
        col = row.find_all('td')
        if len(col) != 0:
            if col[1].find('a') is not None:
                all_links = col[1].find_all('a')
                data_dict = {"Name": all_links[1].contents[0], "MC_USD_Billion": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df_e = pd.concat([df_e, df1], ignore_index=True)

    df_e["MC_USD_Billion"] = df_e["MC_USD_Billion"].str.strip().astype(float)
    return df_e


def transform(df_e, csv_path):
    df = pd.read_csv(csv_path)
    exchange_dic = df.set_index("Currency")["Rate"].to_dict()
    df_e["MC_GBP_Billion"] = [np.round( x * exchange_dic['GBP'], 2) for x in df_e['MC_USD_Billion']]
    df_e["MC_EUR_Billion"] = [np.round(x * exchange_dic['EUR'], 2) for x in df_e['MC_USD_Billion']]
    df_e["MC_INR_Billion"] = [np.round(x * exchange_dic['INR'], 2) for x in df_e['MC_USD_Billion']]

    return df_e


def load_to_csv(df_e, outpath):
    df_e.to_csv(outpath, index=False)


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_cmd, sql_connection):
    print(query_cmd)
    query_output = pd.read_sql(query_cmd, sql_connection)
    print(query_output)


url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_atrribts_e = ["Name", "MC_USD_Billion"]
csv_path = "./exchange_rate.csv"
outpath = "./banks.csv"
table_name = 'Largest_banks'
db_name = 'Banks.db'


log_progress('Preliminaries complete. Initiating ETL process')

df_e = extract(url, table_atrribts_e)
log_progress('Data extraction complete. Initiating Transformation process')

df_e = transform(df_e, csv_path)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df_e, outpath)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')

load_to_db(df_e, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')

query_cmd1 = f"SELECT * FROM {table_name}"
run_query(query_cmd1, sql_connection)

query_cmd2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_cmd2, sql_connection)

query_cmd3 =  f"SELECT Name from {table_name} LIMIT 5"
run_query(query_cmd3, sql_connection)

sql_connection.close()
