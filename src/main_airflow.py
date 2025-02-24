from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.extract import WebScraper
from etl.transform import TransformData
from etl.load import DataFrameToSQLServer

years = ["2021", "2022", "2023"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

dag = DAG(
    "etl_antaq_pipeline",
    default_args=default_args,
    schedule_interval="@monthly",  
    catchup=False,
)

def extract_data(**kwargs):
    for year in years:
        url = "https://web3.antaq.gov.br/ea/sense/download.html#pt"
        download_path = f"../data/zip/{year}.zip"
        unzip_dir = f"../data/unzip/{year}"

        scraper = WebScraper(url, year, download_path, unzip_dir)
        scraper.setup_driver()
        scraper.open_page()
        scraper.select_year()
        download_link = scraper.find_download_link()
        scraper.download_file(download_link)
        scraper.unzip_file()
        scraper.close_driver()

def transform_data(**kwargs):
    for year in years:
        unzip_dir = f"../data/unzip/{year}"
        
        transform = TransformData()
        atracacao_fato = transform.transform_atracacao_fato(year, unzip_dir)

    kwargs['ti'].xcom_push(key='atracacao_fato', value=atracacao_fato)

def load_data(**kwargs):
    ti = kwargs['ti']
    atracacao_fato = ti.xcom_pull(key='atracacao_fato', task_ids='transform')

    load = DataFrameToSQLServer()
    load.save_to_sql(atracacao_fato, "atracacao_fato", "append")

# Criando as tarefas
extract_task = PythonOperator(
    task_id="extract",
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform",
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load",
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task
