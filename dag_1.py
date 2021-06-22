from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.hooks.postgres_hook import PostgresHook
from task_6 import task6
from airflow.models import Variable


def suma(**kwargs):
    var1 = kwargs['arg1']
    var2 = kwargs['arg2']
    result = var1 + var2
    print('El var1 es: ', var1)
    print('El var12 es: ', var2)
    print('El result es: ', result)

    # postgres_conn = PostgresHook(postgres_conn_id=kwargs['posrgres1'])
    # postgres_conn = postgres_conn.get_conn()
    # postgres_cursor = postgres_conn.cursor()
    # postgres_cursor.execute('select * form table;')
    # rows = postgres_cursor.fetch_one()


    return {
        'result': result,
        'status': 'OK'
    }

def imprimir(**kwargs):
    res = kwargs['task_instance'].xcom_pull(task_ids='task_4')
    print('El XCOM de la tarea4 es ', res['result'])

    return

args = {
    'owner': 'Diego',
    'depends_on_past': False,
    'email': ['diego@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(hours=1),
}


dag_backup = DAG(
    'midas_backup_dag',
    default_args=args,
    start_date=datetime(2021, 6, 22, 13, 59, 0),
    end_date=None,
    schedule_interval=None,
    tags=['tutorial', 'radicaciones', 'tag'],
    dagrun_timeout=timedelta(minutes=10)
)

task1 = BashOperator(
    task_id='print_date',
    dag=dag_backup,
    bash_command='date',
)

task2 = PythonOperator(
    task_id='task_2',
    dag=dag_backup,
    python_callable=suma,
    op_kwargs={'arg1': 1, 'arg2': 2}
)

task3 = PythonOperator(
    task_id='task_3',
    dag=dag_backup,
    python_callable=suma,
    op_kwargs={'arg1': 1, 'arg2': 2}
)

task4 = PythonOperator(
    task_id='task_4',
    dag=dag_backup,
    python_callable=suma,
    op_kwargs={'arg1': 1, 'arg2': 2}
)
var1 = Variable.get('var1', default_var='', deserialize_json=False)
task5 = PythonOperator(
    task_id=var1,
    dag=dag_backup,
    python_callable=imprimir,
    op_kwargs={'arg1': 1, 'arg2': 2}
)

task6.dag = dag_backup

var2 = Variable.get('var2', default_var='', deserialize_json=False)

task1 >> task2
task2 >> task3
task3 >> [task4, task5]