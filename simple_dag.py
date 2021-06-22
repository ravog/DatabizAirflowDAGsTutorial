"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the task logs to avoid those getting too big.
airflow trigger_dag --conf '[curly-braces]"maxLogAgeInDays":30[curly-braces]' airflow-log-cleanup
--conf options:
    maxLogAgeInDays:<INT> - Optional
"""
from datetime import datetime, timedelta


import jinja2
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'simple_dag',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2021, 5, 9, 23, 45),
)
if hasattr(dag, 'doc_md'):
    dag.doc_md = __doc__
if hasattr(dag, 'catchup'):
    dag.catchup = False

inicio = DummyOperator(
    task_id='inicio',
    dag=dag)

task1 = DummyOperator(
    task_id='tarea_1',
    dag=dag)

task2 = DummyOperator(
    task_id='tarea_2',
    dag=dag)

task3 = DummyOperator(
    task_id='tarea_3',
    dag=dag)

task4 = DummyOperator(
    task_id='tarea_4',
    dag=dag)

task5 = DummyOperator(
    task_id='tarea_5',
    dag=dag)

task6 = DummyOperator(
    task_id='tarea_6',
    dag=dag)

task7 = DummyOperator(
    task_id='tarea_7',
    dag=dag)

task8 = DummyOperator(
    task_id='tarea_8',
    dag=dag)

task9 = DummyOperator(
    task_id='tarea_9',
    dag=dag)

task10 = DummyOperator(
    task_id='tarea_10',
    dag=dag)

inicio >> task1
inicio >> task8
task1 >> task2
[task2, task3, task4] >> task5
task5 >> [task6, task7]
task8 >> task10
[task7, task6] >> task9
task9 >> task10
