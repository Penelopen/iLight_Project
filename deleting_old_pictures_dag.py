from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from hdfs import InsecureClient
from concurrent.futures import ThreadPoolExecutor

def hdfs_photo_cleaning():
    pg_hook = PostgresHook(postgres_conn_id='greenplum_conn')

    hdfs_target_dir = '/hdfs/media/products/'
    hdfs_conn = BaseHook.get_connection('hdfs_conn')
    hdfs_client = InsecureClient(f'http://{hdfs_conn.host}:{hdfs_conn.port}/', user=hdfs_conn.login)

    # 1. Забираем список ЖИВЫХ картинок и превращаем в Set
    actual_images = pg_hook.get_records("SELECT image_name FROM marts.mv_actual_images;")
    actual_images_set = {row[0] for row in actual_images if row and row[0]}

    # 2. Читаем всё, что реально лежит в папке на HDFS
    try:
        all_hdfs_files = set(hdfs_client.list(hdfs_target_dir))
    except Exception:
        return

    # 3. Из всех вычитаем актуальные - остаётся мусор. Сразу клеим полный путь к файлу
    garbage_files = all_hdfs_files - actual_images_set

    # 4. Клеим пути только для мусора и отправляем в потоки
    files_to_delete = [hdfs_target_dir + name for name in garbage_files]

    if not files_to_delete:
        print(f'Файлов в HDFS: {len(all_hdfs_files)}. Мусора нет, всё нужно 😋')
        return

    # 4. Функция удаления одного файла мимо корзины
    def delete_file(path):
        try:
            hdfs_client.delete(path, recursive=False, skip_trash=True)
        except Exception as e:
            print(f"WARN: Ошибка удаления {path}: {e}")

    # 5. Удаляем параллельно в 32 потока
    with ThreadPoolExecutor(max_workers=32) as executor:
        executor.map(delete_file, files_to_delete)

    print(f'Удалено фвйлов в HDFS: {len(garbage_files)} из {len(all_hdfs_files)}.')

def check_idle_suppliers():
    pg_hook = PostgresHook(postgres_conn_id='greenplum_conn')

    not_actual_suppliers = pg_hook.get_records("select * from marts.v_idle_suppliers;")

    if not_actual_suppliers:
        suppliers_list_html = "<br>".join([f"ID: {row[0]} ({row[1]}). Последняя загрузка: {row[2]}" for row in not_actual_suppliers])
        print(f'Простаивающих поставщиков: {len(not_actual_suppliers)}')
    else:
        suppliers_list_html = '✔️ Простаивающих поставщиков нет'
        print(suppliers_list_html)

    hook = SmtpHook(smtp_conn_id='smtp_conn')
    with hook.get_conn() as smtp_client: # Нужно обязательно инициализировать smtp_client в воздух
        hook.send_email_smtp(
            to='7029293@gmail.com',
            subject='Простаивающие поставщики',
            html_content=f'{suppliers_list_html}'
        )

defaults = {
    'start_date': datetime(2026, 6, 3, 0, 0), # ПОСЛЕ какой даты обрабатывать данные
    'owner': 'TonyB', # Устанавливает владельца таски. Общепринято 'airflow'
##    'trigger_rule': 'all_success, all_done, one_failed, none_failed, dummy' # Другие условия запуска
    'depends_on_past': False, # Запуск текущей задачи зависит от её предыдущего завершения
    'retries': 3,                             # Количество повторений при ошибке
    'retry_delay': timedelta(minutes=1),      # Пауза между попытками
    'retry_exponential_backoff': True,        # Время следующей попытки x2. Дать БД ожить
    'max_retry_delay': timedelta(minutes=15), # Ограничение (чтобы не ждать вечность)
    'execution_timeout': timedelta(minutes=30) # Убить зависшую задачу и пометить Failed
##    'sla': timedelta(hours=1),  # Ожидаемое время выполнения задачи. Превышение = письмо
##    'sla_miss_callback': my_function(),  # При превышении вызвать функцию
##    'email': ['de@company.com', 'admin@company.com'], # Почта настраивается в airflow.cfg
##    'email_on_failure': True,  # Слать письмо при ошибке
##    'email_on_retry': False,   # Не спамить при перезапусках
##    'on_failure_callback'[on_success_callback]: my_func() # Падение/Успех вызвает функцию
}

with DAG('weekly_cleaning',
    default_args=defaults,
    schedule='0 23 * * 5', # Каждую пятницу в 23:00
    max_active_runs=1, # Лимит на 1 экземпляр этого DAG (за разные даты) одновременно
    catchup=False # Выполнять-ли задачи для пропущенных интервалов с момента start_date
) as dag:

# 1. Раз в неделю удаляем картинки, которых нет в БД. Рецепт: Список всех файлов в HDFS минус список marts.mv_actual_images
    task1 = PythonOperator(
        task_id='hdfs_photo_cleaning',
        python_callable=hdfs_photo_cleaning
    )

# 2. Сразу проверяем простаивающих поставщиков и алертим
    task2 = PythonOperator(
        task_id='check_idle_suppliers',
        python_callable=check_idle_suppliers,
        trigger_rule='all_done'
    )

    task1 >> task2
