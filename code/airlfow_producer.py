import requests
from json import dumps
from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

API_KEY = '*'
SYMBOL = 'INTC'
KAFKA_BROKER = '*'   
TOPIC_NAME = 'stockdata_demo'

def get_and_push_stock_data():
    url = f"https://api.stockdata.org/v1/data/quote?symbols={SYMBOL}&api_token={API_KEY}"

    try:
        # --- 1. Gọi API ---
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        quote = data["data"][0]

        

        # --- 2. Gửi dữ liệu lên Kafka ---
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: dumps(v).encode('utf-8')
        )
        producer.send(TOPIC_NAME, value=quote)
        producer.flush()
        print(f"[SENT] {SYMBOL} data sent to Kafka topic '{TOPIC_NAME}'")

    except Exception as e:
        print(f"[ERROR] Pipeline failed: {e}")
        raise e


# --------- DAG Config ---------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date':datetime(2025, 11, 3),
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_data_single_task',
    default_args=default_args,
    description='Fetch stock data from API and push directly to Kafka (single task)',
    schedule='@daily',  
    catchup=False,
)

fetch_and_push_task = PythonOperator(
    task_id='fetch_and_push_to_kafka',
    python_callable=get_and_push_stock_data,
    dag=dag,
)
