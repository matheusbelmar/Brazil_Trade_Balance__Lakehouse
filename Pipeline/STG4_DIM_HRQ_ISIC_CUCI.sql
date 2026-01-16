INSERT INTO iceberg.gold.hrq_isic_cuci
SELECT DISTINCT  
       co_isic_secao,
       no_isic_secao,
       co_cuci_pos,
       no_cuci_pos
FROM iceberg.silver.br_setex_bot_isic_cuci
WHERE
	dt_ingest = (SELECT MAX(dt_ingest) FROM iceberg.silver.br_setex_bot_isic_cuci)