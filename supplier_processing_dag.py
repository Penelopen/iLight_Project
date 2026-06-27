from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
import handler
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

defaults = {
    'start_date': datetime(2025, 7, 1, 0, 0),
    'owner': 'TonyB',
    'trigger_rule': 'all_success',
    'depends_on_past': False, # Запуск таски зависит от её предыдущего успешного завершения
    'retries': 1,                             # Количество повторений при ошибке
    'retry_delay': timedelta(minutes=1),      # Пауза между попытками
    'retry_exponential_backoff': True,        # Время следующей попытки x2. Дать БД ожить
    'max_retry_delay': timedelta(minutes=1),  # Ограничение (чтобы не ждать вечность)
    'execution_timeout': timedelta(minutes=2) # Убить зависшую задачу и пометить Failed
##    'sla': timedelta(minutes=2),  # Ожидаемое время выполнения задачи. Превышение = письмо
##    'sla_miss_callback': my_function(),  # При превышении вызвать функцию
##    'on_failure_callback'[on_success_callback]: my_func() # Падение/Успех вызвает функцию
}

with DAG('iLight_processing',
    default_args=defaults,
    description='iLight suppliers processing',
    schedule='*/2 * * * *', # DAG запускается каждые 2 минуты
    max_active_runs=1, # Лимит на 1 экземпляр этого DAG (за разные даты) одновременно
    catchup=False, # Выполнять-ли задачи для пропущенных интервалов с момента start_date
    template_searchpath=['/opt/airflow/plugins']
) as dag:

    clear_temp_dirs = BashOperator(
        task_id='clear_images_data_directories',
        bash_command='rm -rf /tmp/images/*; mkdir -p /tmp/images/; rm -f /tmp/data/batch.csv'
    )

    task1 = ConsumeFromTopicOperator(
        task_id='consume_kafka_batch',
        kafka_config_id='kafka_conn',
        topics=['supplier_catalog'],
        max_messages=4000,          # Забрать максимум 4к строк
        max_batch_size=4000,         # По сколько обрабатывать функцией, вызывая её каждый раз. Желательно = max_messages, чтобы 1 раз вызывать.
        poll_timeout=2.0,            # Завершиться, если нет новых сообщений в течение 5 сек.
        apply_function_batch=handler.process_and_load_to_greenplum, # Выполнить Функцию из файла, когда батч нальётся
        commit_cadence='never'
    )

    task2 = PythonOperator(
        task_id='pictures_to_HDFS',
        python_callable=handler.upload_images_to_hdfs,
        execution_timeout=timedelta(minutes=5)
    )

    task3 = SQLExecuteQueryOperator(
        task_id='data_to_greenplum',
        conn_id='greenplum_conn',
        do_xcom_push=False,
        sql="marts_load.sql"
    )

    task4 = PythonOperator(
        task_id='clear_and_commit_kafka_batch',
        python_callable=handler.commit_offsets
    )

    clear_temp_dirs >> task1 >> task2 >> task3 >> task4
