from datetime import datetime
from datetime import timedelta

import pandas as pd
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

SRC_CONN = 'postgres_default'

CSV = '/app/airflow/files/Month.csv'

TABLE_IN_QUERY = "select * from public.flight_delays"


def join_table():
    table2 = pd.read_csv(CSV, delimiter=';')  # Считайте файл значений, разделенных запятыми (csv), в DataFrame.
    postgres_hook = PostgresHook(postgres_conn_id=SRC_CONN)
    engine = postgres_hook.get_sqlalchemy_engine()
    table1 = pd.read_sql(
        TABLE_IN_QUERY,
        engine)  # запрос из БД
    table1.columns = ['MONTH_CODE', 'DAYOFMONTH', 'DAYOFWEEK', 'DEPTIME', 'UNIQUECARRIER', 'ORIGIN',
                      'DEST', 'DISTANCE']

    print(table1.head(5).to_string())

    table2.columns = ['MONTH_CODE', 'MONTH_NAME']

    print(table2.to_string())

    df = pd.merge(table1, table2, how='left', on=['MONTH_CODE'])

    print(df.head(5).to_string())
    # df2 = df[["MONTH_CODE", "DAYOFMONTH", "DAYOFWEEK", "DEPTIME", "UNIQUECARRIER", 'ORIGIN',
                      # 'DEST', 'DISTANCE', "MONTH_NAME"]]

    # dataTypeSeries = df2.dtypes

    # df2.to_sql('garanin_ans', con=engine, if_exists='replace', chunksize=500, index=False)

    return 'OK'


dag = DAG('DAG_GARANIN', description='join_table', schedule_interval=timedelta(days=1),
          start_date=datetime(2017, 3, 20), catchup=False)

hello_operator = PythonOperator(task_id='task_garanin', python_callable=join_table, dag=dag)
