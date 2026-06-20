import json
import random
import base64
import hashlib
from io import BytesIO
from PIL import Image, ImageDraw
from kafka import KafkaProducer

# ---------- НАСТРОЙКИ ----------
topic_name = 'supplier_catalog'
num_records = 30000 # Количество сгенерирванных строк
max_pics = 15 # Максимальное количество картинок в товаре
max_delivery = 120 # Максимальный срок поставки, дн.
max_suppliers = 100 # Максимальное коичество постащиков
min_price, max_price = 500.0, 50000.0 # Диапазон цен товаров
POISON_PROCENT = 0.4  # Процент испорченных строк (мусор в типах данных). 0.3 - 30% мусора, 1.0 - 100% БАТЧА КРИВЫЕ
DUPLICATE_PHOTO_PROCENT = 0.8
    # 0.0 - все картинки 100% уникальные (генерируются на лету)
    # 0.5 - половина картинок берется из пула дубликатов, половина генерируется заново
    # 1.0 - ВСЕ картинки берутся только из пула (максимум одинаковых хэшей)
# -------------------------------

def generate_mock_image_base64():
    bg_color = (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
    img = Image.new('RGB', (200, 200), color=bg_color)
    draw = ImageDraw.Draw(img)
    draw.rectangle([random.randint(10, 50), random.randint(10, 50),
                    random.randint(100, 190), random.randint(100, 190)],
                   fill=(random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)))
    buffer = BytesIO()

    # Для стабильности хэшей при дублировании используем PNG (JPEG может давать микро-шумы)
    img_format = 'PNG'
    img.save(buffer, format=img_format)
    img_bytes = buffer.getvalue()
    b64_encoded = base64.b64encode(img_bytes).decode('utf-8')
    mime_type = f"data:image/{img_format.lower()};base64,"
    return f"{mime_type}{b64_encoded}"

def send_mock_data_to_kafka():
    producer = KafkaProducer(bootstrap_servers='192.168.205.128:9092', acks=1, retries=5)
    product_names = ['Прожектор LED IP65', 'Светильник трековый', 'Лента светодиодная RGB', 'Диммер сенсорный', 'Контроллер шлюза']
    categories = [10, 12, 15, 22, 33]

    # Списки для генерации названий поставщиков
    supplier_adjectives = ['Глобал', 'Пром', 'Техно', 'Электро', 'Свет', 'Опт', 'Евро', 'Интер', 'Нью', 'Люкс']
    supplier_nouns = ['Трейд', 'Снаб', 'Торг', 'Системс', 'Групп', 'Комплект', 'Импорт', 'Маркет', 'Лайт', 'Индустрия']

    # Создаём пул из 15 картинок, которые МОГУТ дублироваться
    shared_image_pool = [generate_mock_image_base64() for _ in range(10)]

    print(f"Старт генерации {num_records} сообщений в топик '{topic_name}'...")
    poisoned_counter = 0

    for i in range(num_records):
        supplier_id = random.randint(1, max_suppliers)
        item_uid = f"light-sku-{random.randint(10000, 99999)}"
        price = round(random.uniform(min_price, max_price), 2)
        category_id = random.choice(categories)

        # Стабильная генерация имени поставщика на основе его числового ID через хэширование
        id_hash = int(hashlib.md5(str(supplier_id).encode('utf-8')).hexdigest(), 16)
        adj_index = (id_hash) % len(supplier_adjectives)
        noun_index = (id_hash // len(supplier_adjectives)) % len(supplier_nouns)
        supplier_name = f"ООО {supplier_adjectives[adj_index]}{supplier_nouns[noun_index]}"

        # Случайный срок поставки
        delivery_days = random.randint(1, max_delivery)

        images_list = []

        # Решаем для всего товара: брать базовую картинку из пула или сделать уникальную
        if random.random() < DUPLICATE_PHOTO_PROCENT:
            static_image = random.choice(shared_image_pool)
        else:
            static_image = generate_mock_image_base64()

        # Заполняем массив картинок для товара
        for _ in range(random.randint(1, max_pics)): #
            # Проверяем процент дубликатов для каждой отдельной фотки в массиве
            if random.random() < DUPLICATE_PHOTO_PROCENT:
                images_list.append(random.choice(shared_image_pool))
            else:
                images_list.append(generate_mock_image_base64())

        # --- КОРРУПЦИЯ ДАННЫХ (ГЕНЕРАТОР ГРЯЗИ) ---
        is_poisoned = random.random() < POISON_PROCENT
        if is_poisoned:
            poisoned_counter += 1
            corruption_type = random.choice(['text_in_price', 'text_in_supplier', 'broken_array'])

            if corruption_type == 'text_in_price':
                price = "ЦЕНА ДОГОВОРНАЯ"
            elif corruption_type == 'text_in_supplier':
                supplier_id = "ДЯДЯ_ВАСЯ_С_БАЗЫ"
                supplier_name = "НЕИЗВЕСТНЫЙ ПОСТАВЩИК"
            elif corruption_type == 'broken_array':
                images_list = "ПРОСТО_КРИВАЯ_СТРОКА_ВМЕСТО_МАССИВА"

        payload = {
            "supplier_id": supplier_id,
            "supplier_name": supplier_name,
            "item_uid": item_uid,
            "title": f"{random.choice(product_names)} {random.randint(100, 900)}W",
            "price": price,
            "category_id": category_id,
            "delivery_days": delivery_days,
            "description": "Промышленное освещение на базе высокоэффективных диодов. Гарантия 5 лет.",
            "images_base64": images_list
        }

        json_bytes = json.dumps(payload).encode('utf-8')
        producer.send(topic_name, json_bytes)

        if i % 500 == 0 and i > 0:
            producer.flush()
            print(f"Отправлено: {i} строк...")

    producer.flush()
    print(f"🎉 Успешно отправлено {num_records} товаров! Из них умышленно испорчено: {poisoned_counter} строк.")

if __name__ == '__main__':
    send_mock_data_to_kafka()
