from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from exchange_parsing import tj_exchange, kz_exchange, uz_exchange, kg_exchange, arm_exchange, az_exchange, by_exchange, mn_exchange, ge_xchange, get_start_end_dates, pred_exchange

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}


with DAG(
    dag_id='currency_parser_dag',
    default_args=default_args,
    description='Парсинг курсов валют по странам СНГ',
    start_date=datetime(2025, 7, 2),
    schedule='0 14 1 * *',
    #schedule='*/15 * * * *',
    catchup=False,
    max_active_runs=1
) as dag:

    headers = {'User_agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36'}
    start_date, end_date = get_start_end_dates()

    task_tj = PythonOperator(
        task_id='tajikistan_exchange',
        python_callable=tj_exchange,
        op_args=[start_date, end_date]
    )

    task_kz = PythonOperator(
        task_id='kazakhstan_exchange',
        python_callable=kz_exchange,
        op_args=[start_date, end_date]
    )

    task_uz = PythonOperator(
        task_id='uzbekistan_exchange',
        python_callable=uz_exchange,
        op_args=[start_date, end_date]
    )

    task_kg = PythonOperator(
        task_id='kyrgyzstan_exchange',
        python_callable=kg_exchange,
        op_args=[start_date, end_date]
    )

    task_arm = PythonOperator(
        task_id='armenia_exchange',
        python_callable=arm_exchange,
        op_args=[start_date, end_date]
    )

    task_az = PythonOperator(
        task_id='azerbaijan_exchange',
        python_callable=az_exchange,
        op_args=[start_date, end_date]
    )

    task_by = PythonOperator(
        task_id='belarus_exchange',
        python_callable=by_exchange,
        op_args=[start_date, end_date]
    )

    task_mn = PythonOperator(
        task_id='mongolia_exchange',
        python_callable=mn_exchange,
        op_args=[start_date, end_date]
    )

    task_ge = PythonOperator(
        task_id='georgia_exchange',
        python_callable=ge_xchange
    )

    task_pred = PythonOperator(
        task_id='prednestr_exchange',
        python_callable=pred_exchange,
        op_args=[start_date, end_date]
    )

    [task_tj, task_kz, task_uz, task_kg, task_arm, task_az, task_by, task_mn, task_ge, task_pred]
