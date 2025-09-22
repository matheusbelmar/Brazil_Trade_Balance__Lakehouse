
DROP SCHEMA iceberg.bronze;
CREATE SCHEMA IF NOT EXISTS iceberg.bronze
WITH (location = 's3a://datalake/bronze/');

DROP SCHEMA iceberg.silver;

CREATE SCHEMA IF NOT EXISTS iceberg.silver
WITH (location = 's3a://datalake/silver');


CREATE TABLE iceberg.silver.br_setex_bot_isic_cuci3 (
    tipo             VARCHAR,
    co_ano           INT,
    co_mes           INT,
    co_isic_secao    VARCHAR,
    no_isic_secao    VARCHAR,
    co_cuci_pos      VARCHAR,
    no_cuci_pos      VARCHAR,
    vl_fob_usd       BIGINT,
    kg_liquido       BIGINT
)WITH (
    partitioning = ARRAY['co_ano','co_mes']
);
SELECT *
FROM iceberg.silver.br_setex_bot_isic_cuci3


FROM iceberg.silver.br_setex_bot_isic_cuci3;