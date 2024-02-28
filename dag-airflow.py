import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
import requests
import time
import boto3
from datetime import datetime, timedelta
from airflow.models import Variable

aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")

# Default settings applied to all tasks
default_args = {
    "owner": "Luiza Costa Fernandes Carneiro",
    "depends_on_past": False,
    "start_date": datetime(2023, 11, 23),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1
}

@dag(
    default_args=default_args,
    dag_id="api_powerbi",
    schedule_interval="0 6,12 * * 1-5",
    catchup=False,
    tags=["api", "powerbi", "dados"],
    description="Pipeline para gerenciamento do ambiente do PowerBI",
)

def pipeline_powerbi():    

    task_raw = KubernetesPodOperator(    
        task_id = "task_to_raw",
        namespace = 'airflow',
        image = 'sua imagem',
        arguments = ['/app/api_powerbi_to_raw.py'],
        name = 'task_to_raw',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    ) 

    task_staging = KubernetesPodOperator(    
        task_id = "task_to_staging",
        namespace = 'airflow',
        image = 'sua imagem',
        arguments = ['/app/api_powerbi_to_staging.py'],
        name = 'task_to_staging',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True,
        image_pull_policy='Always'
    )    

    task_consumer = KubernetesPodOperator(
        task_id="task_to_consumer",
        namespace='airflow',
        image='sua imagem',
        arguments=['/app/api_powerbi_to_consumer.py'],
        name='task_to_consumer',
        env_vars={'AWS_ACCESS_KEY_ID': aws_access_key_id,
                  'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod=True,
        in_cluster=True,
        get_logs=True,
        image_pull_policy='Always'
    )

    # Control Flow      
    task_raw >> task_staging >> task_consumer
    
execucao = pipeline_powerbi()


