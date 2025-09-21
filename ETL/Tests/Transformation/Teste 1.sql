
DROP SCHEMA iceberg.bronze;
CREATE SCHEMA IF NOT EXISTS iceberg.bronze
WITH (location = 's3a://datalake/bronze/');

DROP SCHEMA iceberg.silver;

CREATE SCHEMA IF NOT EXISTS iceberg.silver
WITH (location = 's3a://datalake/silver');

CREATE TABLE IF NOT EXISTS iceberg.silver.br_setex_tb_ISIC_CUCI (
    tipo             VARCHAR,
    co_ano           INT,
    co_mes           INT,
    co_isic_secao    VARCHAR,
    no_isic_secao    VARCHAR,
    co_cuci_pos      VARCHAR,
    no_cuci_pos      VARCHAR,
    vl_fob_usd       DECIMAL(18,2),
    kg_liquido       DECIMAL(18,2)
)
WITH (
    format = 'parquet',
    location = 's3a://datalake/silver/br_setex_tb_ISIC_CUCI'
	);


CREATE TABLE iceberg.bronze.test_table (
    id INT
)
WITH (
    format = 'PARQUET',
    location = 's3://datalake/test_table/'
);

