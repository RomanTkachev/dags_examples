from airflow.hooks.base import BaseHook
from airflow.models import Variable

API_URL = "https://b2b.itresume.ru/api/statistics"

def load_from_api (**context):
    import requests 
    import pendulum
    import psycopg2 as pg

    payload = {
        "client": Variable.get("ITRESUME_CLIENT"),
        "client_key": Variable.get("ITRESUME_CLIENT_KEY"),
        "start": context['ds'],
        "end": pendulum.parse(context['ds']).add(days=1).to_date_string(),
    }

    response = requests.get(API_URL, params=payload)
    data = response.json()

    connection = BaseHook.get_connection('conn_pg')