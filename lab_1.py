# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(url):
    data = requests.get(url)
    return (data.json())


@task
def transform(stock_1, stock_2, data1, data2):
    results = [] # empyt list for now to hold the 90 days of stock info (open, high, low, close, volume)
    for d in data1["Time Series (Daily)"]: # here d is a date: "YYYY-MM-DD"
        stock_info = data1["Time Series (Daily)"][d]
        stock_info['6. date'] = d
        results.append({'0. stock': stock_1} | stock_info)
        if len(results)>89:
          break
    for d in data2["Time Series (Daily)"]: # here d is a date: "YYYY-MM-DD"
        stock_info = data2["Time Series (Daily)"][d]
        stock_info['6. date'] = d
        results.append({'0. stock': stock_2} | stock_info)
        if len(results)>179:
          break
    return results

@task
def load(con, records, target_table):
    try:
        con.execute("BEGIN;")
        con.execute(f"DROP TABLE IF EXISTS {target_table};")
        con.execute(f"CREATE OR REPLACE TABLE {target_table} (stock string, open float, high float, low float, close float, volume int, date DATE primary key);")
        for r in records:
            stock = r["0. stock"]
            open = r["1. open"]
            high = r["2. high"]
            low = r["3. low"]
            close = r["4. close"]
            volume = r["5. volume"]
            date = r["6. date"]
            sql = f"INSERT INTO {target_table} (stock, open, high, low, close, volume, date) VALUES ('{stock}', {open}, {high}, {low}, {close}, {volume}, '{date}')"
            con.execute(sql)
        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id = 'stocks',
    start_date = datetime.now(),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    target_table = Variable.get("table")
    url_1 = Variable.get("stock_1")
    url_2 = Variable.get("stock_2")
    stock_1 = Variable.get("symbol_1")
    stock_2 = Variable.get("symbol_2")
    cur = return_snowflake_conn()

    data1 = extract(url_1)
    data2 = extract(url_2)
    records = transform(stock_1, stock_2, data1, data2)
    load(cur, records, target_table)
