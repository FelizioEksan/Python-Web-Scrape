import pandas as pd  
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import sqlite3

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attributes = ['Name' , 'MC_USD_Billion']
target_file = "Largest_banks_data.csv"
db = "Banks.db"
table_name = "Largest_banks"
log_file = "code_log.txt"

def extract(url , table_attributes):
    df = pd.DataFrame(columns=table_attributes)
    page = requests.get(url).text
    data = BeautifulSoup(page , 'html.parser')
    table = data.find_all('tbody')
    rows = table[0].find_all('tr')
    for row in rows[0:] : 
        col = row.find_all('td')
        if len(col) != 0: 
            if col[1].find('a') is not None:
                name = col[1].get_text(strip=True)
                mc_usd = col[2].get_text(strip=True)
                data_dict = {'Name': name , 'MC_USD_Billion':mc_usd}
                df1 = pd.DataFrame(data_dict , index=[0])
                df = pd.concat([df,df1] , ignore_index=True)
    return df

def transform(df): 
    df["MC_USD_Billion"] = df["MC_USD_Billion"].apply(lambda x: float(x.replace(',' , '').replace('\n' , '')))

    exchange_df = pd.read_csv("exchange_rate.csv")
    exchange_df = exchange_df.set_index("Currency").to_dict()["Rate"]

    df["MC_GBP_Billion"] = np.round(df["MC_USD_Billion"] * exchange_df["GBP"] , 2)
    df["MC_INR_Billion"] = np.round(df["MC_USD_Billion"] * exchange_df["INR"] , 2)
    df["MC_EUR_Billion"] = np.round(df["MC_USD_Billion"] * exchange_df["EUR"] , 2)

    return df

def load_to_csv(df , target_file):
    df.to_csv(target_file , index=False)

def load_to_sql(df , db , table_name) : 
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    df.to_sql(table_name , conn , if_exists='replace' , index=False)

    query1 = 'SELECT * FROM Largest_banks'
    cursor.execute(query1)
    for row in cursor.fetchall():
        print(row)
    print("\n")
    query2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
    cursor.execute(query2)
    for row in cursor.fetchall():
        print(row[0])
    print("\n")
    query3 = 'SELECT Name from Largest_banks LIMIT 5'
    cursor.execute(query3)
    for row in cursor.fetchall():
        print(row[0])
    print("\n")
    conn.close()

def log_progress(expression):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt" , "a") as f: 
        f.write(timestamp + ' : ' + expression + '\n')

#Run SC 

log_progress("Program start")

log_progress("Extracting")
df = extract(url , table_attributes)
print("Raw Data: \n")
print(df)
log_progress("Extract function complete")
print("\n")

log_progress("Transforming")
tdf = transform(df)
print("Transformed Data:\n")
print(tdf)
log_progress("Transform function complete")

print("\n")

log_progress("Loading to csv")
load_to_csv(tdf , target_file)
log_progress("Load to csv complete")

log_progress("Loading to SQL")
load_to_sql(tdf , db , table_name)
log_progress("Load to SQL complete")

log_progress("Program ended")

