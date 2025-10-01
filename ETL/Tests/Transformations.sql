WITH
LAST_PARTITION AS (
	SELECT 
		   tipo
		  ,co_ano
		  ,co_mes
		  ,co_isic_secao
		  ,no_isic_secao
		  ,co_cuci_pos
		  ,no_cuci_pos
		  ,vl_fob_usd
		  ,kg_liquido
		  ,dt_ingest
		  ,RANK() OVER(ORDER BY dt_ingest DESC) AS PT		  
	FROM iceberg.silver.br_setex_bot_isic_cuci
	)
,STG AS (
	SELECT 
		 tipo
		,co_ano
		,co_mes
		,no_isic_secao
		,SUM(kg_liquido)  AS kg_liquido
		,SUM(vl_fob_usd)  AS vl_fob_usd
	FROM LAST_PARTITION
	WHERE
		PT=1
	GROUP BY 
		tipo,
		co_ano,
		co_mes,
		no_isic_secao
	)
	SELECT 
		   *
		,SUM(vl_fob_usd ) OVER(
			PARTITION BY co_ano, tipo, no_isic_secao 
			ORDER BY co_mes 
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW 
			)	AS ytd_vl_fob_usd
		FROM STG 
		ORDER BY 2 DESC, 3 DESC, 4, 1
	
	
	
	
	
