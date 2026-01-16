import os
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "rest",
    uri="http://localhost:8181",
    **{
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        }
    )
table_fato       = catalog.load_table(r"gold.br_setex_bot_isic_cuci_fato")
table_dim_isic   = catalog.load_table(r"gold.hrq_isic_cuci")

full_fato = table_fato.scan().to_arrow()
full_dim_isic = table_dim_isic.scan().to_arrow()


pq.write_table(full_fato, r"extração/fato_TB.parquet", compression="snappy")
pq.write_table(full_dim_isic, r"extração/isic_cuci_dim.parquet", compression="snappy")


import pyarrow.csv as csv

csv.write_csv(
    full_fato,
    "extração/fato_TB.csv"
)

csv.write_csv(
    full_dim_isic,
    "extração/isic_cuci_dim.csv"
)

Base_Full_v2=full_fato.join(full_dim_isic, keys="co_cuci_pos", join_type="inner")

csv.write_csv(
    Base_Full_v2,
    "extração/FATO.csv"
)