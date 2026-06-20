import os
import shutil
import json
import base64
import hashlib
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from hdfs import InsecureClient
from airflow.operators.python import get_current_context
from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
from confluent_kafka import TopicPartition
from airflow.exceptions import AirflowSkipException

images_temp_dir = '/tmp/images/'
hdfs_target_dir = '/hdfs/media/products/'

text_data_batch = []
all_batch_suppliers = set() # Для EXCHANGE партиций
new_suppliers = set()
items_count = 0
new_images_count = 0
kafka_offsets = {}

# Забираем метаданные из XCom
def xcom_pull():
    global items_count, new_images_count, kafka_offsets
    ti = get_current_context()['ti']

    # Т.к. этот скрипт может запуститься несколько раз за одну таску, то накапливаем значения с каждым запуском скрипта:
    try:
        all_batch_suppliers.update(ti.xcom_pull(task_ids='consume_kafka_batch', key='all_batch_suppliers') or [])
        new_suppliers.update(ti.xcom_pull(task_ids='consume_kafka_batch', key='new_suppliers') or [])
        items_count = ti.xcom_pull(task_ids='consume_kafka_batch', key='items_count') or 0
        new_images_count = ti.xcom_pull(task_ids='consume_kafka_batch', key='new_images_count') or 0
        kafka_offsets = ti.xcom_pull(task_ids='consume_kafka_batch', key='kafka_batch_offsets') or {}
    except Exception as e:
        print(f'Ошибка чтения XCom (нормально для 1-го батча): {e}')

    print(f'Начало микробатча. Получено картинок из XCom: {new_images_count}')

# ФУНКЦИЯ ДЛЯ ТАСКА 1: ОБРАБОТКА БАТЧА ИЗ KAFKA И СВЯЗЬ С GREENPLUM
def process_and_load_to_greenplum(messages, **context): # messages — батч сырых JSON-строк из Kafka
    global items_count, new_images_count, kafka_offsets
    # НАСТРОЙКА ПУТЕЙ И КЛИЕНТОВ
    tmp_csv_path = '/tmp/data/'
    tmp_csv_filename = os.path.join(tmp_csv_path, 'batch.csv')
    os.makedirs(os.path.dirname(tmp_csv_path), exist_ok=True)

    pg_hook = PostgresHook(postgres_conn_id='greenplum_conn')

# 1. ДЁРГАЕМ GREENPLUM ОДИН РАЗ НА СТАРТЕ БАТЧА И ЗАБИРАЕМ, ЧТО ЕСТЬ в XCOM
    ti = get_current_context()['ti']

    # --- ВЫГРЕБАЕМ СПИСОК 1: КАРТИНКИ ---
    gp_actual_images = pg_hook.get_records("SELECT * FROM marts.mv_actual_images;")
    # Сразу забираем результат первого запроса в память через fetchall()
    actual_images_in_ram = {row[0].split('/')[-1] for row in gp_actual_images if row and row[0]}

    # --- ВЫГРЕБАЕМ СПИСОК 2: ПОСТАВЩИКИ ---
    gp_actual_suppliers = pg_hook.get_records("SELECT DISTINCT partitionlistvalues FROM pg_partitions WHERE tablename = 'target_prices_ao_column';")
    # Приводим к инту, страхуемся от дефолтной партиции (other_suppliers) проверкой isdigit()
    actual_suppliers_in_ram = {int(row[0].strip("'")) for row in gp_actual_suppliers if row and row[0] and row[0].strip("'").isdigit()}

    actual_db_images_count = len(actual_images_in_ram)

    xcom_pull()

# 2. ЦИКЛ: ДЕРБАНИМ СЫРЫЕ JSON-СООБЩЕНИЯ ИЗ KAFKA
    for msg in messages:
        try:
            payload = json.loads(msg.value().decode('utf-8'))

            supplier_id = payload['supplier_id']
            supplier_name = payload['supplier_name']
            item_uid = payload['item_uid']
            title = payload['title']
            price = payload['price']
            category_id = payload['category_id']
            delivery_days = payload['delivery_days']
            description = payload['description']
            images_base64_list = payload.get('images_base64', [])

            if not supplier_id or not str(supplier_id).strip().isdigit():
                print(f'Пропущена строка. supplier_id имеет некорректный формат: {supplier_id}')
                continue

            # Проверяем только то, что это в принципе можно превратить в число
            try:
                float(str(price).replace(',', '.').strip())
            except (ValueError, TypeError):
                print(f'Пропущена строка. price имеет некорректный формат: {price}')
                continue

            try:
                int(float(str(delivery_days).replace(',', '.').strip()))
            except (ValueError, TypeError):
                print(f'Пропущена строка. delivery_days имеет некорректный формат: {delivery_days}')
                continue

            # Ограничение на количество картинок одного товара
            if isinstance(images_base64_list, list) and len(images_base64_list) > 3:
                print(f'Пропущена строка. Товар {item_uid} содержит более 3 картинок ({len(images_base64_list)})')
                continue

            # Перебираем картинки одного товара
            product_image_names = set()

            for b64_string in images_base64_list:
                if ',' in b64_string:
                    header, b64_data = b64_string.split(',', 1)
                    ext = header.split(';')[0].split('/')[-1]
                else:
                    b64_data = b64_string
                    ext = 'jpg'

                # Декодируем Base64 в байты памяти воркера
                image_bytes = base64.b64decode(b64_data)

                # Генерируем имя по MD5 от контента
                image_hash = hashlib.md5(image_bytes).hexdigest()
                image_name = f'{image_hash}.{ext}'

                # ПРОВЕРКА СУЩЕСТВОВАНИЯ КАРТИНКИ
                if image_name not in actual_images_in_ram:
                    # СКЛАДЫВАЕМ ВО ВРЕМЕННУЮ ПАПКУ НА ВОРКЕРЕ
                    with open(os.path.join(images_temp_dir, image_name), 'wb') as img_file:
                        img_file.write(image_bytes)

                    # Добавляем в список, защищаясь от дублей внутри этой же пачки
                    actual_images_in_ram.add(image_name)
                    new_images_count += 1

                # Путь в манифест пишем в любом случае
                product_image_names.add(image_name)

            # Добавляем поставщиков в списки для нарезки и обмена партиций
            if supplier_id not in actual_suppliers_in_ram:
                new_suppliers.add(int(supplier_id))
            all_batch_suppliers.add(int(supplier_id))

            # Собираем массив имён в формат Greenplum: {image1,image2}
            pg_array_format = "{" + ",".join(product_image_names) + "}"

            text_data_batch.append([
                supplier_id, supplier_name, item_uid, title, price, category_id, delivery_days, description, pg_array_format
            ])

        except Exception as e:
            print(f'Ошибка строки Kafka: {e}. Пропускаем.')
            print(payload)
            continue

    print(f'Батч содержит {len(messages)} строк')

    if not text_data_batch:
        print('WARN: Обработка завершена. Батч не содержит валидных строк.')
    else:
# 3. ПОСЛЕ ЦИКЛА — СБРОС ТЕКСТА В CSV ДЛЯ GPFDIST
        with open(tmp_csv_filename, 'a', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            writer.writerows(text_data_batch)

        items_count = len(text_data_batch)
        print(f'Обработка завершена. Итого {items_count} строк добавлены в CSV.')

# 4. ОБНОВЛЯЕМ СПИСКИ в XCOM
    if messages:
        for msg in messages:
            key = f'{msg.topic()}:{msg.partition()}'
            kafka_offsets[key] = max(kafka_offsets.get(key, 0), msg.offset() + 1)
    ti.xcom_push(key="new_images_count", value=new_images_count)
    ti.xcom_push(key="items_count", value=items_count)
    ti.xcom_push(key="all_batch_suppliers", value=list(all_batch_suppliers))
    ti.xcom_push(key="new_suppliers", value=list(new_suppliers))
    ti.xcom_push(key="new_suppliers_count", value=len(new_suppliers))
    ti.xcom_push(key="kafka_batch_offsets", value=kafka_offsets)

    print(f'Новых изображений: {new_images_count}')
    print(f'Новые поставщики ({len(new_suppliers)}): {new_suppliers}')
    print(f'Текущие оффсеты: {kafka_offsets}')
    return

# ФУНКЦИЯ ДЛЯ ТАСКА 2: БАТЧЕВАЯ ЗАГРУЗКА В HDFS И ОЧИСТКА
def upload_images_to_hdfs():
    xcom_pull()

    # Если в Kafka новых сообщений нет, то XCom пуст. Загружать нечего, скипаем таску
    if not kafka_offsets:
        raise AirflowSkipException('WARN: Кафка пустая. Новых сообщений нет. Скипаем загрузку и коммит.')

    # Если временной папки нет или она пустая — загружать нечего, выходим
    if not os.path.exists(images_temp_dir) or not os.listdir(images_temp_dir):
        print('WARN: Временная папка пуста. Нет картинок для загрузки в HDFS. Пропускаем.')
        return

    conn = BaseHook.get_connection('hdfs_conn')

    # Подключаемся напрямую к NameNode HDFS по порту WebHDFS
    hdfs_client = InsecureClient(f'http://{conn.host}:{conn.port}/', user=conn.login)

    # Заливаем всю папку целиком со всеми файлами за один HTTP-стрим
    hdfs_client.upload(hdfs_target_dir, images_temp_dir, overwrite=True)
    print(f"Батч картинок ({new_images_count}) успешно загружен в HDFS.")

    # Очистка временной папки
    shutil.rmtree(images_temp_dir, ignore_errors=True)
    print('Временная папка', images_temp_dir, 'успешно очищена.')

# ФУНКЦИЯ ДЛЯ УДАЛЕНИЯ CSV И КОММИТА ОФФСЕТОВ
def commit_offsets():
    xcom_pull()

    # Удаляем отработанный CSV-файл с воркера
    if os.path.exists('/tmp/data/batch.csv'):
        os.remove('/tmp/data/batch.csv')

    if not kafka_offsets:
        return

    # Извлекаем имя топика из первого ключа для инициализации хука
    any_topic = list(kafka_offsets.keys())[0].split(':')[0] if kafka_offsets else 'default_topic'
    consumer = KafkaConsumerHook(topics=[any_topic], kafka_config_id='kafka_conn').get_consumer()

    # Парсим плоскую структуру словаря оффсетов {"topic:partition": offset}
    tps = [TopicPartition(key.split(':')[0], int(key.split(':')[1]), int(val)) for key, val in kafka_offsets.items()]

    try:
        consumer.commit(offsets=tps, asynchronous=False)
    finally:
        consumer.close()
        print('Коммит в Kafka выполнен, мой повелитель 😎')
