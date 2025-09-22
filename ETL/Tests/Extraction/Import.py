import os
import s3fs
import pandas as pd
from pyarrow import Table, parquet as pq
pd.set_option("display.max_columns", None)

# Scraping
link=r"https://balanca.economia.gov.br/balanca/SH/ISIC_CUCI.xlsx"
path=r"/home/mbelmar/Documentos/Programaçao - Projetos/Projetos/Brazil Trade Balance - Lakehouse/ETL/Tests/Ingestion/Data/ISIC_CUCI.xlsx"

df_TB_ISIC_CUCI=pd.read_excel(path, sheet_name="DADOS_ISIC_CUCI")
#Data_TB_ISIC_CUCI=pd.read_excel(link, sheet_name="DADOS_ISIC_CUCI")


# minIO configs
fs = s3fs.S3FileSystem(
    anon=False,                   
    key="admin",                  
    secret="password",            
    endpoint_url="http://localhost:9000", 
    use_ssl=False                  
)

bucket_name = "s3://datalake"
prefix = "bronze/trade_balance/"  # onde você quer salvar
file_name="ISIC_BR_Data.parquet"

minio_path=os.path.join(bucket_name, prefix, file_name)

#print(minio_path)
#print(df_TB_ISIC_CUCI.columns)

#print(fs.ls("datalake"))

#print(fs.ls("/"))

# Export
pq.write_to_dataset(
    Table.from_pandas(df_TB_ISIC_CUCI),
    minio_path,
    filesystem=fs,
    use_dictionary=True,
    compression="snappy"
    #version="2.0",
)
