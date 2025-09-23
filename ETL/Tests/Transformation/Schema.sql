
-- Step 1
CREATE SCHEMA IF NOT EXISTS iceberg.bronze
WITH (location = 's3a://datalake/bronze/');

-- Step 2
CREATE SCHEMA IF NOT EXISTS iceberg.silver
WITH (location = 's3a://datalake/silver');

-- Step 3
CREATE TABLE iceberg.silver.br_setex_bot_isic_cuci (
    tipo             VARCHAR,
    co_ano           INT,
    co_mes           INT,
    co_isic_secao    VARCHAR,
    no_isic_secao    VARCHAR,
    co_cuci_pos      VARCHAR,
    no_cuci_pos      VARCHAR,
    vl_fob_usd       BIGINT,
    kg_liquido       BIGINT,
    dt_ingest			TIMESTAMP
)WITH (
    partitioning = ARRAY['co_ano','dt_ingest']
);



