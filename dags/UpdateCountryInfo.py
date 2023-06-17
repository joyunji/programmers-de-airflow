from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

import logging
import requests
from datetime import datetime

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def get_country_info(url):
  json_data = None
  records = []
  response = requests.get(url)
  if response.status_code == 200:
    json_data = response.json()
  for record in json_data:
    records.append([record["name"]["official"], record["population"], record["area"]])
  return records
    
@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
CREATE TABLE {schema}.{table} (
    country varchar,
    population bigint,
    area float
);""")
        
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES ('{r[0]}',{r[1]},{r[2]});"
            print(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    
    logging.info("load done")

with DAG(
    dag_id='UpdateCountryInfo',
    start_date = datetime(2023,6,16),
    catchup=False,
    tags=['API'],
    schedule= '30 6 * * SAT'
) as dag:
    
    url = Variable.get('restcountries_api_url')
    schema = 'dbtmxlsk2007'
    table = 'countryinfo'
    
    results = get_country_info(url)
    load(schema, table, results)
    
