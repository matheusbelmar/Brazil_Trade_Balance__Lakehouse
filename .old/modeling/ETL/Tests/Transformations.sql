INSERT INTO iceberg.gold.br_setex_bot_isic_cuci_fato
SELECT 
	   co_ano
	  ,tipo
	  ,CO_MES
	  ,CAST(concat(CAST(co_ano AS VARCHAR), lpad(CAST(co_mes AS VARCHAR), 2, '0')) AS INTEGER) AS TMP_CD
	  ,CO_CUCI_POS
	  ,VL_FOB_USD
	  ,SUM(VL_FOB_USD)
	   OVER (
	   	PARTITION BY co_ano, co_cuci_pos, tipo
	   	ORDER BY co_mes 
	   	ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	   	) AS VL_FOB_USD_YTD
	   ,SUM(VL_FOB_USD)
	    OVER (
	   	PARTITION BY co_cuci_pos, tipo
	   	ORDER BY co_ano, co_mes 
	   	ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
	   	) AS VL_FOB_USD_12M
	   ,kg_liquido
	   ,SUM(kg_liquido)
	    OVER (
	   	PARTITION BY co_ano, co_cuci_pos, tipo
	   	ORDER BY co_mes 
	   	ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	   	) AS kg_liquido_YTD
	   ,SUM(kg_liquido)
	    OVER (
	   	PARTITION BY co_cuci_pos, tipo
	   	ORDER BY co_ano, co_mes 
	   	ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
	   	) AS kg_liquido_12M
FROM iceberg.silver.br_setex_bot_isic_cuci 
WHERE
		dt_ingest = (SELECT MAX(dt_ingest) FROM iceberg.silver.br_setex_bot_isic_cuci)
