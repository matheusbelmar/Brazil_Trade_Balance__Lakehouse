import os
import s3fs
import requests as req

# Scraping
url=r"https://balanca.economia.gov.br/balanca/SH/ISIC_CUCI.xlsx"

# minIO configs
fs = s3fs.S3FileSystem(
    anon=False,                   
    key="admin",                  
    secret="password",            
    endpoint_url="http://localhost:9000", 
    use_ssl=False                  
    )

response = req.get(url)
response.raise_for_status()  

bucket_name = "s3://datalake"
prefix = "bronze/trade_balance/"  
file_name="ISIC_BR_Data.xlsx"

minio_path=os.path.join(bucket_name, prefix, file_name)

with fs.open(minio_path, "wb") as handle:
    handle.write(response.content)


#Scrap >> Landing_Bronze >> Iceberg_Curation_Silver