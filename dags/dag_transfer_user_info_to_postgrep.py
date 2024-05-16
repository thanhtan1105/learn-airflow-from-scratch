import json
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from faker import Faker
import random
import os, sys

default_args = {
    'owner': 'tanle'
}


@dag(default_args=default_args, schedule_interval="@daily", start_date=days_ago(2), tags=['learn'], catchup=False)
def dag_transfer_user_info_to_postgrep():
    @task()
    def generate_data():
        data_structure = {
            'customerid': None,
            'firstname': None,
            'lastname': None,
            'email': None,
            'balance': None,
            'account_type': None,
            'transaction_id': None,
            'amount': None,
            'transaction_type': None,
            'transaction_date': None
        }
        output = []
        fake = Faker()
        for _ in range(1000):
            data_structure['customerid'] = fake.uuid4()
            data_structure['firstname'] = fake.first_name()
            data_structure['lastname'] = fake.last_name()
            data_structure['email'] = fake.email()
            data_structure['balance'] = random.randint(100, 10000)
            data_structure['account_type'] = random.choice(['savings', 'checking'])
            data_structure['transaction_id'] = fake.uuid4()
            data_structure['amount'] = random.randint(1, 100)
            data_structure['transaction_type'] = random.choice(['credit', 'debit'])
            data_structure['transaction_date'] = fake.date_time_this_year()
            output.append(data_structure.copy())

        return output

    @task
    def transform(data):
        customer_entity = []
        account_entity = []
        transaction_entity = []
        for row in data:
            customer_entity.append({
                'firstname': row['firstname'],
                'lastname': row['lastname'],
                'email': row['email']
            })

            account_entity.append({
                'customerid': row['customerid'],
                'balance': row['balance'],
                'account_type': row['account_type']
            })

            transaction_entity.append({
                'transaction_id': row['transaction_id'],
                'customerid': row['customerid'],
                'amount': row['amount'],
                'transaction_type': row['transaction_type'],
                'transaction_date': row['transaction_date']
            })

        return {
            'customer': customer_entity,
            'account': account_entity,
            'transaction': transaction_entity
        }

    @task
    def create_table_postgres():
        with open(os.path.join(str(os.path.dirname(__file__)).replace('/dags', ''), f'plugins/init_script.sql')) as f:
            init_script = f.read()

        PostgresOperator(
            task_id='create_table',
            postgres_conn_id='postgres_bank_db',
            sql=init_script
        ).execute(context=None)

    @task
    def load_to_postgres(data):
        print(data)
        pass

    data = generate_data()
    create_table = create_table_postgres()
    transformed_data = transform(data)
    load_to_postgres(transformed_data)

    create_table >> transformed_data


dag_transfer_user_info_to_postgrep()
