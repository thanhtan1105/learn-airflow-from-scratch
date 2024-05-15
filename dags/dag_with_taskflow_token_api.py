import json
import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from utils.coingecko import Coingecko
import time

default_args = {
    'owner': 'tanle'
}

@dag(default_args=default_args, schedule_interval="@daily", start_date=days_ago(2), tags=['example'])
def dag_with_taskflow_token_api():
    coingecko = Coingecko()

    @task()
    def extract():
        source_url = coingecko.get_config()['token']['list_url']
        response = requests.get(source_url)
        data = json.loads(response.text)
        return data

    @task(multiple_outputs=True)
    def add_price_to_token(data):
        tokens = ','.join(token['symbol'] for token in data['tokens'])
        token_price = coingecko.get_token_price_by_address(tokens)
        for token in data['tokens']:
            token['price'] = token_price[token['symbol']]
        return data

    @task(multiple_outputs=True)
    def load(data_add_price):
        print(json.dumps(data_add_price))

    data = extract()
    data_add_price = add_price_to_token(data)
    load(data_add_price)


dag_with_taskflow_token_api = dag_with_taskflow_token_api()