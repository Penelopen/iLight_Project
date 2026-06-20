/*
Я выбрал Jinja патамушта он прозрачнее для таски, чем DO $$ самой базы, который является сплошной транзакцией. Пока рефрешится вьюха,
транзакция остается открытой, и витрина заблокирована! Более того, один косячный поставщик может откатить всю эту транзакцию по заливке.
В Jinja, когда дело дойдёт до REFRESH MATERIALIZED VIEW, блокировки с витрины уже будут сняты. Пусть Jinja шлёт изолированные команды...
*/

-- Защита от 404: Весь SQL-код выполнится ТОЛЬКО если в батче реально есть хотя бы один товар!
{% set items_count = ti.xcom_pull(task_ids='consume_kafka_batch', key='items_count') | int %}
{% if items_count and items_count > 0 %}

-- Ограничиваем время ожидания блокировок (если база занята аналитиками, висеть не будем, а уроним таску)
SET lock_timeout = '10s';

-- 1. Дорезаем новые партиции, если в батче появились новые поставщики. Поставщиков берём из XCom
{% set new_suppliers_list = ti.xcom_pull(task_ids='consume_kafka_batch', key='new_suppliers') | default([], true) %}
{% if (new_suppliers_list | length) > 0 %}
  {%- for supplier_id in new_suppliers_list %}
  ALTER TABLE marts.target_prices_ao_column ADD PARTITION p_supplier_{{supplier_id}} VALUES ({{supplier_id}});
  {% endfor %}
{% endif %}

TRUNCATE stage.suppliers_ao_row;

-- 2. Заливаем батч из CSV в Stage через gpfdist и причёсываем данные
INSERT INTO stage.suppliers_ao_row (supplier_id, supplier_name, item_uid, title, price, category_id, delivery_days, description, image_paths)
WITH ranked as (SELECT supplier_id, supplier_name, item_uid, title, price, category_id, delivery_days, description, image_paths
      , row_number() over(partition by supplier_id, item_uid, category_id, price) as rn
FROM stage.external_table_gpfdist
WHERE supplier_id IS NOT NULL
AND supplier_name IS NOT NULL
AND item_uid IS NOT NULL AND item_uid != ''
AND price::numeric(15,2) > 0
AND length(title) >= 3           -- Игнорируем товары с подозрительно коротким названием
AND delivery_days IS NOT NULL AND delivery_days::int >= 0
AND category_id IS NOT NULL
)
SELECT supplier_id, supplier_name, item_uid, title, price, category_id, delivery_days, description, image_paths
FROM ranked WHERE rn = 1;

-- 3. Делаем обмен партициями между таргет и временной таблицами
{% set all_batch_suppliers_list = ti.xcom_pull(task_ids='consume_kafka_batch', key='all_batch_suppliers') | default([], true) %}

-- Поочередный EXCHANGE в цикле для каждого поставщика
DO $$
DECLARE
    v_supplier INT;
    v_suppliers_list INT[];
    v_raw_list TEXT := '{% if all_batch_suppliers_list %}{{ all_batch_suppliers_list | join(",") }}{% endif %}';
BEGIN
    IF v_raw_list != '' THEN
        v_suppliers_list := string_to_array(v_raw_list, ',')::int[];

        FOREACH v_supplier IN ARRAY v_suppliers_list LOOP
            -- а) Каждый раз создаём новую пустую таблицу-буфер со структурой витрины. Чтобы не мешали скрытые чеки
            EXECUTE 'DROP TABLE IF EXISTS stage.tmp_single_exch_buffer CASCADE';
            EXECUTE 'CREATE TABLE stage.tmp_single_exch_buffer (LIKE marts.target_prices_ao_column)
                     WITH (appendonly=true, orientation=column) DISTRIBUTED BY (supplier_id)';

            -- б) Вешаем проверочный CHECK текущего поставщика, который база требует для легального обмена
            EXECUTE format('ALTER TABLE stage.tmp_single_exch_buffer ADD CONSTRAINT chk_curr CHECK (supplier_id = %s)', v_supplier);

            -- в) Из Stage-1 забираем строки СТРОГО одного текущего поставщика
            EXECUTE format('INSERT INTO stage.tmp_single_exch_buffer
                            SELECT supplier_id::int, supplier_name::varchar(100), item_uid, title::varchar(100), price::numeric(15, 2), category_id::int, delivery_days::int, description, load_timestamp, image_paths
                            FROM stage.suppliers_ao_row WHERE supplier_id = ''%s''', v_supplier);

            -- г) Вычищаем всё из целевой партиции, чтобы не было сюрпризов
            EXECUTE format('ALTER TABLE marts.target_prices_ao_column TRUNCATE PARTITION p_supplier_%s', v_supplier);            
            
            -- д) Делаем рокировку - EXCHANGE. Теперь в витрине ТОЛЬКО новые строки!
            EXECUTE format('ALTER TABLE marts.target_prices_ao_column EXCHANGE PARTITION p_supplier_%s
                    WITH TABLE stage.tmp_single_exch_buffer WITHOUT VALIDATION', v_supplier);

          END LOOP;
    END IF;
END $$;

-- 4. Чистим мусор
TRUNCATE stage.suppliers_ao_row;
DROP TABLE IF EXISTS stage.tmp_single_exch_buffer;

-- 5. Обновляем вьюху с картинками, только если приехали новые
{% set new_images_count = ti.xcom_pull(task_ids='consume_kafka_batch', key='new_images_count') | int %}
{% if new_images_count and new_images_count > 0 %}
REFRESH MATERIALIZED VIEW CONCURRENTLY marts.mv_actual_images;
{% endif %}

{% else %}
  SELECT 'WARN: Батч пустой, скипаем gpfdist и обработку';
{% endif %}

