from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.sensor import SqlSensor
from airflow.utils.dates import days_ago
import random
import time
from datetime import datetime, timedelta

# 1.створення таблиці
def create_table(**kwargs):
    kwargs['ti'].xcom_push(key='table_created', value=True)
    return """
    CREATE TABLE IF NOT EXISTS medal_counts (
        id SERIAL PRIMARY KEY,
        medal_type VARCHAR(10),
        count INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

# 2.випадковий вибір медалі
def random_medal_type(**kwargs):
    medal_type = random.choice(['Bronze', 'Silver', 'Gold'])
    kwargs['ti'].xcom_push(key='medal_type', value=medal_type)
    return medal_type

# 3-4. підрахунок кількості медалей
def count_medals(**kwargs):
    medal_type = kwargs['ti'].xcom_pull(task_ids='random_medal_type', key='medal_type')
    count_query = f"SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = '{medal_type}';"
    result = execute_query(count_query)
    kwargs['ti'].xcom_push(key=f'{medal_type}_count', value=result[0][0])

# 5. затримка
def sleep_task(**kwargs):
    time.sleep(35)

# 6. сенсор для перевірки часу створення запису
def sensor_task(**kwargs):
    table_exists = SqlSensor(
        task_id='check_table_exists',
        sql="SELECT COUNT(*) FROM medal_counts WHERE created_at >= NOW() - INTERVAL '30 seconds';",
        mode='poke',
        timeout=40,
        poke_interval=5
    )
    return table_exists

# DAG
dag = DAG(
    'medal_count_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple DAG for counting medals',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)

# 1.створення таблиці
create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)

# 2.випадковий вибір типу медалі
random_medal_type_task = PythonOperator(
    task_id='random_medal_type',
    python_callable=random_medal_type,
    dag=dag,
)

# 3-4.підрахунок медалей
count_bronze_task = PythonOperator(
    task_id='count_bronze',
    python_callable=count_medals,
    dag=dag,
)

count_silver_task = PythonOperator(
    task_id='count_silver',
    python_callable=count_medals,
    dag=dag,
)

count_gold_task = PythonOperator(
    task_id='count_gold',
    python_callable=count_medals,
    dag=dag,
)

# 5.затримка
sleep_task = PythonOperator(
    task_id='sleep_task',
    python_callable=sleep_task,
    provide_context=True,
    dag=dag,
)

# 6. сенсор для перевірки запису
sensor_task = PythonOperator(
    task_id='sensor_task',
    python_callable=sensor_task,
    provide_context=True,
    dag=dag,
)

# Послідовність завдань
create_table_task >> random_medal_type_task
random_medal_type_task >> [count_bronze_task, count_silver_task, count_gold_task]
[count_bronze_task, count_silver_task, count_gold_task] >> sleep_task
sleep_task >> sensor_task

