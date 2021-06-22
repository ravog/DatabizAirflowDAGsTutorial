from airflow.operators.python import PythonOperator

def resta(**kwargs):
    var1 = kwargs['arg1']
    var2 = kwargs['arg2']
    result = var1 - var2
    print('El var1 es: ', var1)
    print('El var12 es: ', var2)
    print('El result es: ', result)

    return

task6 = PythonOperator(
    task_id='task_6',
    python_callable=resta,
    op_kwargs={'arg1': 1, 'arg2': 2}
)