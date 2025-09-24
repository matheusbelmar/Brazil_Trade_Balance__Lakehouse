# Data Lakehouse em Medallion Architecture - Balança Comercial Brasileira

Este projeto implementa uma arquitetura de **Data Lakehouse** baseada no padrão **Medallion Architecture (Bronze, Silver e Gold)** para dados da Balança Comercial Brasileira.

## Arquitetura

- **Armazenamento:** MinIO  
- **Formato de tabelas:** Apache Iceberg  
- **Catálogo:** Iceberg REST  
- **Query Engine:** Trino  
- **Transformações:** dbt  
- **Orquestração:** Apache Airflow  
- **Deploy:** Docker Compose  

## Pipeline

1. **Bronze:** Ingestão bruta dos dados com `s3fs` e `PyArrow`  
2. **Silver:** Normalização e registro das tabelas em Iceberg com `PyIceberg`  
3. **Gold:** Transformações analíticas com `dbt` sobre Trino  

## Ferramentas

- **Python:** `pyarrow`, `s3fs`, `pyiceberg`  
- **Infra:** MinIO, Trino, Iceberg REST, Docker, Airflow, dbt  

---

⚠️ Projeto em desenvolvimento. A versão atual cobre ingestão até a camada Silver, com transformações iniciais em dbt.
