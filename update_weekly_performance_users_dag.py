# O intuito deste script é criar um processo de carga no airflow

import os
from os import path
from datetime import datetime, timedelta

from airflow.hooks.postgres_hook import PostgresHook
#from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import DAG

from modules.common import slack

slack = slack.Slack(slack_connection='alerts-ba-slack', channel='#alertas-ba')

#import graph_tpv

"""
Dag owner - Business Intelligence

"""

#Find current path file and the folder one level above
main_dir = path.dirname(__file__)
up_one_dir = path.dirname(main_dir)

#Identify path to  SQL queries and database for connection
queries_path = 'sql/weekly_performance_report/update_tables'
database = 'airfox-analytics-database'

#Set up folder for files staging
TEMP_FOLDER = path.join(up_one_dir, 'file_staging', 'update_weekly_performance_users_dag')
if not path.exists(TEMP_FOLDER):
    os.mkdir(TEMP_FOLDER)

#Get database connection
postgres_hook = PostgresHook(postgres_conn_id='airfox-analytics-database')
DB_CONN = postgres_hook.get_conn()
DB_CONN.autocommit = True

# Set up arguments and operators -----------------------------------------------
default_args = {
    'owner': 'airfox',
    'start_date':  datetime(2023, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'on_failure_callback': slack.task_fail_alert
}

dag = DAG(
    dag_id='update_weekly_performance_users_dag',
    default_args=default_args,
    schedule_interval='0 10 * * *',
    max_active_runs=1
)

# Define queries to run dict
dict_queries = {
    'registration_origin':                      'registration_origin.sql', 
    'new_users':                                'new_users.sql',
    'user_cohorts':                             'user_cohorts.sql',
    'active_users_products':                    'active_users_products.sql', 
    'ativos_30dias_funcionalidades_canal':      'ativos_30dias_funcionalidades_canal.sql', 
    'active_users_30days_ep':                   'active_users_30days_ep.sql', 
    'active_users_30days_ep_source_channel':    'active_users_30days_ep_source_channel.sql',
    'active_users_90_products':                 'active_users_90_products.sql', 
    'active_users_90days_ep':                   'active_users_90days_ep.sql', 
    'active_users_90days_ep_source_channel':    'active_users_90days_ep_source_channel.sql',
    'first_deposit':                            'first_deposit.sql', 
    'first_spend':                              'first_spend.sql',
    'rps_ativacao_conta_origin':                'rps_ativacao_conta_origin.sql',
    'rps_ativacao_conta_mensal':                'rps_ativacao_conta_mensal.sql',
    'reactivation_after_90days':                'reactivation_after_90days.sql',
    'rps_reativacao':                           'rps_reativacao.sql'
    
}

def get_query_content(query_file):
    with open(query_file, 'r') as file:
        return file.read()

#Tasks for updating tables -----------------------------------------------------
registration_origin = PostgresOperator(
    task_id='update_registration_origin_data',
    sql=get_query_content(path.join(main_dir, queries_path, dict_queries['registration_origin'])),
    postgres_conn_id=database,
    dag=dag
)

new_users = PostgresOperator(
    task_id='update_new_users_data',
    sql=get_query_content(path.join(main_dir, queries_path, dict_queries['new_users'])),
    postgres_conn_id=database,
    dag=dag
)

user_cohorts = PostgresOperator(
    task_id='update_user_cohorts_data',
    sql=get_query_content(path.join(main_dir, queries_path, dict_queries['user_cohorts'])),
    postgres_conn_id=database,
    dag=dag
)

active_users_products = PostgresOperator(
    task_id='update_active_users_products_data',
    sql=get_query_content(path.join(main_dir, queries_path, dict_queries['active_users_products'])),
    postgres_conn_id=database,
    dag=dag
)

ativos_30dias_funcionalidades_canal = PostgresOperator(
    task_id='update_ativos_30dias_funcionalidades_canal_data',
    sql=get_query_content(path.join(main_dir, queries_path, dict_queries['ativos_30dias_funcionalidades_canal'])),
    postgres_conn_id=database,
    dag=dag
)

active_users_30days_ep = PostgresOperator(
    task_id='update_active_users_30days_ep_data',
    sql=get_query_content(path.join(main_dir, queries_path, dict_queries['active_users_30days_ep'])),
    postgres_conn_id=database,
    dag=dag
)

active_users_30days_ep_source_channel = PostgresOperator(
    task_id='update_active_users_30days_ep_source_channel_data',
    sql=get_query_content(path.join(main_dir, queries_path, dict_queries['active_users_30days_ep_source_channel'])),
    postgres_conn_id=database,
    dag=dag
)

active_users_90_products = PostgresOperator(
    task_id='update_active_users_90_products_data',
    sql=get_query_content(path.join(main_dir, queries_path, dict_queries['active_users_90_products'])),
    postgres_conn_id=database,
    dag=dag
)

active_users_90days_ep = PostgresOperator(
    task_id='update_active_users_90days_ep_data',
    sql=get_query_content(path.join(main_dir, queries_path, dict_queries['active_users_90days_ep'])),
    postgres_conn_id=database,
    dag=dag
)

active_users_90days_ep_source_channel = PostgresOperator(
    task_id='update_active_users_90days_ep_source_channel_data',
    sql=get_query_content(path.join(main_dir, queries_path, dict_queries['active_users_90days_ep_source_channel'])),
    postgres_conn_id=database,
    dag=dag
)

first_deposit = PostgresOperator(
    task_id='update_first_deposit_data',
    sql=get_query_content(path.join(main_dir, queries_path, dict_queries['first_deposit'])),
    postgres_conn_id=database,
    dag=dag
)

first_spend = PostgresOperator(
    task_id='update_first_spend_data',
    sql=get_query_content(path.join(main_dir, queries_path, dict_queries['first_spend'])),
    postgres_conn_id=database,
    dag=dag
)

rps_ativacao_conta_origin = PostgresOperator(
    task_id='update_rps_ativacao_conta_origin_data',
    sql=get_query_content(path.join(main_dir, queries_path, dict_queries['rps_ativacao_conta_origin'])),
    postgres_conn_id=database,
    dag=dag
)

rps_ativacao_conta_mensal = PostgresOperator(
    task_id='update_rps_ativacao_conta_mensal_data',
    sql=get_query_content(path.join(main_dir, queries_path, dict_queries['rps_ativacao_conta_mensal'])),
    postgres_conn_id=database,
    dag=dag
)

reactivation_after_90days = PostgresOperator(
    task_id='update_reactivation_after_90days_data',
    sql=get_query_content(path.join(main_dir, queries_path, dict_queries['reactivation_after_90days'])),
    postgres_conn_id=database,
    dag=dag
)

rps_reativacao = PostgresOperator(
    task_id='update_rps_reativacao_data',
    sql=get_query_content(path.join(main_dir, queries_path, dict_queries['rps_reativacao'])),
    postgres_conn_id=database,
    dag=dag
)

registration_origin >> new_users
new_users >> user_cohorts
user_cohorts >> active_users_products
active_users_products >> ativos_30dias_funcionalidades_canal
ativos_30dias_funcionalidades_canal >> active_users_30days_ep
active_users_30days_ep >> active_users_30days_ep_source_channel
active_users_30days_ep_source_channel >> active_users_90_products
active_users_90_products >> active_users_90days_ep
active_users_90days_ep >> active_users_90days_ep_source_channel
active_users_90days_ep_source_channel >> first_deposit
first_deposit >> first_spend
first_spend >> rps_ativacao_conta_origin
rps_ativacao_conta_origin >> rps_ativacao_conta_mensal
rps_ativacao_conta_mensal >> reactivation_after_90days
reactivation_after_90days >> rps_reativacao
