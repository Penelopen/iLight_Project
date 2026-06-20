drop external table external_table_gpfdist;
CREATE EXTERNAL TABLE external_table_gpfdist (
    supplier_id TEXT,
    supplier_name TEXT,
    item_uid TEXT,
    title TEXT,
    price TEXT,
    category_id TEXT,
    delivery_days TEXT,
    description TEXT,
    image_paths TEXT[]
)
LOCATION ('gpfdist://airflow-airflow-worker-1:8082/batch.csv')
FORMAT 'CSV' (DELIMITER ',')
LOG ERRORS
SEGMENT REJECT LIMIT 100 PERCENT;

DROP TABLE stage.suppliers_ao_row cascade;
CREATE TABLE suppliers_ao_row (
    supplier_id text NULL,
    supplier_name text NULL,
    item_uid text NULL,
    title text NULL,
    price text NULL,
    category_id text NULL,
    delivery_days text NULL,
    description text NULL,
    load_timestamp timestamp default (now() AT TIME ZONE 'MSK'),
    image_paths text[] NULL
)
WITH (appendonly=true, orientation=row)
DISTRIBUTED BY (supplier_id);

--------------
DROP TABLE marts.target_prices_ao_column cascade;
create table marts.target_prices_ao_column (
    supplier_id int,
    supplier_name varchar(100),
    item_uid text,
    title varchar(100),
    price numeric(15, 2),
    delivery_days int,
    category_id int,
    description text,
    load_timestamp timestamp,
    image_paths text[]
    )
WITH (appendonly=true,orientation=column)
PARTITION BY LIST (supplier_id)
(PARTITION p_init VALUES (0)); -- Стартовая партиция, чтобы обойти ошибку depth 1

DROP MATERIALIZED VIEW IF EXISTS marts.mv_actual_images;
CREATE MATERIALIZED VIEW marts.mv_actual_images as SELECT DISTINCT unnest(image_paths) AS image_name FROM marts.target_prices_ao_column
DISTRIBUTED BY (image_name);
CREATE UNIQUE INDEX idx_mv_actual_images ON marts.mv_actual_images (image_name);

DROP VIEW IF EXISTS marts.v_idle_suppliers;
CREATE VIEW marts.v_idle_suppliers as
select supplier_id, supplier_name, date_trunc('seconds', max(load_timestamp))::text as ts
from marts.target_prices_ao_column
group by supplier_id, supplier_name
having max(load_timestamp) < (now() AT TIME ZONE 'MSK') - interval '7 days'
order by ts;
--------------

with sex as (select supplier_id, item_uid, title, price, category_id, unnest(image_paths) as image from marts.target_prices_ao_column)
select a.image
    ,a.supplier_id as a_supplier_id, b.supplier_id as b_supplier_id
    ,a.item_uid as a_item_uid, b.item_uid as b_item_uid
    ,a.title as a_title, b.title as b_title
    ,a.price as a_price, b.price as b_price
    ,a.category_id as a_category_id, b.category_id as b_category_id
from sex a
join sex b using(image)
where a.supplier_id != b.supplier_id or a.item_uid != b.item_uid or a.price != b.price or a.category_id != b.category_id


select count(*) from (SELECT distinct unnest(image_paths) AS image_name FROM marts.target_prices_ao_column) t
select count(*) from (SELECT unnest(image_paths) AS image_name FROM marts.target_prices_ao_column) t
select * from marts.target_prices_ao_column order by 1
select * from stage.external_table_gpfdist order by 1
select * from stage.suppliers_ao_row order by 1
select count(*) from marts.mv_actual_images
select * from marts.mv_actual_images
select * from marts.v_idle_suppliers;
REFRESH MATERIALIZED VIEW CONCURRENTLY marts.mv_actual_images

SELECT DISTINCT partitionlistvalues, partitionname FROM pg_partitions WHERE tablename = 'target_prices_ao_column';
SELECT gp_segment_id, tableoid::regclass, supplier_id, item_uid, title FROM marts.target_prices_ao_column

