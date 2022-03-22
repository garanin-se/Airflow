from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

args = {
    'owner': 'garanin',
    'start_date': datetime(2018, 11, 1),
    'provide_context': True}  # настройки дага, которые перадаем каждому дагу через дефолтный аргумент

with DAG("Hello_world", description='Hello_world', schedule_interval='*/1 * * * *', catchup=False,
         default_args=args) as dag:
    # "hello-world" - название дага
    # description - описание дага
    # */1 * * * * - каждую минуту Cron
    # catchup - если true будет исполняться каждую минуту без пауз с момента datetime(2018, 11, 1)
    t1 = BashOperator(
        task_id='task_1',
        bash_command='echo "Hello World from Task 1"')

    t2 = BashOperator(
        task_id='task_2',
        bash_command='echo "Hello World from Task 2"')

    t3 = BashOperator(
        task_id='task_3',
        bash_command='echo "Hello World from Task 3"')

    t4 = BashOperator(
        task_id='task_4',
        bash_command='echo "Hello World from Task 4"')

    t1 >> t2
    t1 >> t3
    t2 >> t4
    t3 >> t4
