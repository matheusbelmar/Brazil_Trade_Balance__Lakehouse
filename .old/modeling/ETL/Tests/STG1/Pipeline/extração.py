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

files_fato      = [f.file.file_path for f in table_fato.scan().plan_files()]
files_dim_isic  = [f.file.file_path for f in table_dim_isic.scan().plan_files()]

pt_tables_fato = [pq.read_table(path) for path in files_fato]
full_fato = pa.concat_tables(pt_tables_fato)

pt_tables_dim_isic = [pq.read_table(path) for path in files_dim_isic]
#full_dim_isic = pa.concat_tables(pt_tables_dim_isic)


pq.write_table(full_fato, r"extração/fato_exp.parquet")
#pq.write_table(full_dim_isic, r"extração/isic_dim.parquet")

