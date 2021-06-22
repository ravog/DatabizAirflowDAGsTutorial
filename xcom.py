"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the task logs to avoid those getting too big.
airflow trigger_dag --conf '[curly-braces]"maxLogAgeInDays":30[curly-braces]' airflow-log-cleanup
--conf options:
    maxLogAgeInDays:<INT> - Optional
"""
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator


def xcom_example(**kwargs):
    var = 1
    print("Ejecucion de {}".format(kwargs['arg1']))
    if kwargs['pull']:
        aux = kwargs['task_instance'].xcom_pull(task_ids='inicio')
        print('Este es el {}'.format(aux))

    return 'XCOM de la {}'.format(kwargs['arg1'])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['hugo.vargas@databiz.com.py'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'xcom_example',
    concurrency=1,
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2021, 6, 21, 10, 23),
    tags=['radicaciones']
)
if hasattr(dag, 'doc_md'):
    dag.doc_md = __doc__
if hasattr(dag, 'catchup'):
    dag.catchup = False

inicio = PythonOperator(
    task_id='inicio',
    python_callable=xcom_example,
    op_kwargs={'arg1': 'inicio', 'pull': False},
    dag=dag)

task1 = PythonOperator(
    task_id='task_1',
    python_callable=xcom_example,
    op_kwargs={'arg1': 'tarea_1', 'pull': True},
    dag=dag)

inicio >> task1