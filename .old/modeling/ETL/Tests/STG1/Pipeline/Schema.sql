
-- Step 1
CREATE SCHEMA IF NOT EXISTS iceberg.bronze
WITH (location = 's3a://datalake/bronze/');
GO


-- Step 2
CREATE SCHEMA IF NOT EXISTS iceberg.silver
WITH (location = 's3a://datalake/silver')
GO

CREATE SCHEMA IF NOT EXISTS iceberg.gold
WITH (location = 's3a://datalake/gold')
GO


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
)
GO 


CREATE TABLE iceberg.gold.hrq_isic_cuci (
    co_isic_secao    VARCHAR,
    no_isic_secao    VARCHAR,
    co_cuci_pos      VARCHAR,
    no_cuci_pos      VARCHAR
	);

DROP TABLE IF EXISTS iceberg.gold.br_setex_bot_isic_cuci_fato
CREATE TABLE iceberg.gold.br_setex_bot_isic_cuci_fato (
     co_ano          INT
    ,co_mes          INT
    ,TMP_CD          INT
    ,co_cuci_pos     VARCHAR
    ,VL_FOB_USD		BIGINT
	 ,VL_FOB_USD_YTD	BIGINT
	 ,VL_FOB_USD_12M	BIGINT
	 ,kg_liquido		BIGINT
	 ,kg_liquido_YTD	BIGINT
	 ,kg_liquido_12M	BIGINT
)WITH (
    partitioning = ARRAY['co_ano']
);
