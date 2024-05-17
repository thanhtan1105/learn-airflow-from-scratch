import json
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
            data_structure['customerid'] = random.randint(1, 20)
            data_structure['firstname'] = fake.first_name()
            data_structure['lastname'] = fake.last_name()
            data_structure['email'] = fake.email()
            data_structure['balance'] = random.randint(100, 10000)
            data_structure['account_type'] = random.choice(['savings', 'checking'])
            data_structure['amount'] = random.randint(1, 100)
            data_structure['transaction_type'] = random.choice(['credit', 'debit'])
            data_structure['transaction_date'] = fake.date_time_this_year()
            output.append(data_structure.copy())

        return output

    @task
    def transform(data):
        customer_entity = {}
        account_entity = {}
        transaction_entity = []

        for row in data:
            customer_entity[row['customerid']] = {
                'firstname': row['firstname'],
                'lastname': row['lastname'],
                'email': row['email']
            }

            account_entity[row['customerid']] = {
                'customerid': row['customerid'],
                'balance': row['balance'],
                'account_type': row['account_type']
            }

            transaction_entity.append({
                'transaction_id': row['transaction_id'],
                'customerid': row['customerid'],
                'amount': row['amount'],
                'transaction_type': row['transaction_type'],
                'transaction_date': row['transaction_date']
            })

        return {
            'customer': list(customer_entity.values()),
            'account': list(account_entity.values()),
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
        # customer entity
        customer_entity_values = ', '.join([f"('{row['firstname']}', '{row['lastname']}', '{row['email']}')" for row in data['customer']])
        customer_insert_sql = f"""INSERT INTO public.customers (firstname, lastname, email) VALUES {customer_entity_values} ON CONFLICT ("email") DO UPDATE SET firstname = EXCLUDED.firstname, lastname = EXCLUDED.lastname;"""
        PostgresOperator(
            task_id='upsert_customer',
            postgres_conn_id='postgres_bank_db',
            sql=customer_insert_sql
        ).execute(context=None)

        # account entity
        account_entity_values = ', '.join([f"('{row['customerid']}', {row['balance']}, '{row['account_type']}')" for row in data['account']])
        account_insert_sql = f"""INSERT INTO public.accounts (customerid, balance, account_type) VALUES {account_entity_values} ON CONFLICT ("customerid") DO UPDATE SET balance = EXCLUDED.balance, account_type = EXCLUDED.account_type;"""
        PostgresOperator(
            task_id='upsert_account',
            postgres_conn_id='postgres_bank_db',
            sql=account_insert_sql
        ).execute(context=None)

        # transaction entity
        data['transaction'] = convert_customer_id_to_account_id(data['transaction'])
        transaction_entity_values = ', '.join([f"({row['account_id']}, {row['amount']}, '{row['transaction_type']}', '{row['transaction_date']}')" for row in data['transaction']])
        print(transaction_entity_values)
        transaction_insert_sql = f"""INSERT INTO public.transactions (account_id, amount, transaction_type, transaction_date) VALUES {transaction_entity_values}"""
        PostgresOperator(
            task_id='upsert_transaction',
            postgres_conn_id='postgres_bank_db',
            sql=transaction_insert_sql
        ).execute(context=None)


    def convert_customer_id_to_account_id(transactions):
        sql = f"""SELECT customerid, account_id from public.accounts WHERE customerid IN ({','.join([str(row['customerid']) for row in transactions])})"""
        hook = PostgresHook(postgres_conn_id='postgres_bank_db')
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        for transaction in transactions:
            for row in rows:
                if transaction['customerid'] == row[0]:
                    transaction['account_id'] = row[1]
                    break
        return transactions




    data = generate_data()
    create_table = create_table_postgres()
    transformed_data = transform(data)
    load_to_postgres(transformed_data)

    create_table >> transformed_data


dag_transfer_user_info_to_postgrep()
