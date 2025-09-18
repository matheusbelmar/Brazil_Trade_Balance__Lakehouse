link=r"https://balanca.economia.gov.br/balanca/SH/ISIC_CUCI.xlsx"


import pyarrow.csv as pv
import pyarrow.parquet as pq

# read CSV into an Arrow Table
table = pv.read_csv("my_data.csv")

# inspect schema
print(table.schema)

# convert to parquet
pq.write_table(table, "my_data.parquet")