import os
import s3fs
import pandas as pd
import pyarrow as pa
from datetime import datetime
from pyiceberg.catalog import load_catalog

def load_from_land (
    schema:     str, 
    file_name:  str,
    sheet:      str,
    now:        datetime
    ) -> pd.DataFrame:

    # minIO configs
    fs = s3fs.S3FileSystem(
            anon=False,                   
            key="admin",                  
            secret="password",            
            endpoint_url="http://localhost:9000", 
            use_ssl=False                  
        )

    # Coordinates
    bucket = "s3://datalake"
    ori_base = os.path.join(bucket, schema, file_name)

    # Loading df
    with fs.open(ori_base, 'rb') as f:
        df = pd.read_excel(f, sheet_name=sheet)
    
    df['dt_ingest']=now
    return df

def pandas_to_arrow(
    df_data: pd.DataFrame
    ) -> pa.table:

    # Old_name -> (New name, type)
    pd_Schema = {
        "TIPO":             ("tipo",          str),
        "CO_ANO" :          ("co_ano",        int),
        "CO_MES" :          ("co_mes",        int),
        "CO_ISIC_SECAO":    ("co_isic_secao", str),
        "NO_ISIC_SECAO":    ("no_isic_secao", str),
        "CO_CUCI_POS":      ("co_cuci_pos",   str),
        "NO_CUCI_POS":      ("no_cuci_pos",   str),
        "US$ VL_FOB":       ("vl_fob_usd",    int),
        "KG_LIQUIDO":       ("kg_liquido",    int),
        "dt_ingest":        ("dt_ingest",     "datetime64[s]")
        }

    df_stg = pd.DataFrame({pd_Schema[i][0]: df_data[i].astype(pd_Schema[i][1]) for i in pd_Schema})

    arrow_schema = pa.schema([
        ('tipo',            pa.string()),
        ('co_ano',          pa.int32()),
        ('co_mes',          pa.int32()),
        ('co_isic_secao',   pa.string()),
        ('no_isic_secao',   pa.string()),
        ('co_cuci_pos',     pa.string()),
        ('no_cuci_pos',     pa.string()),
        ('vl_fob_usd',      pa.int64()),
        ('kg_liquido',      pa.int64()),
        ('dt_ingest',       pa.timestamp('s'))
        ])

    return pa.Table.from_pandas(df_stg, schema=arrow_schema, preserve_index=False)

def up_till_iceberg(
    arrow_table: pa.table   
    ) -> None:

    # Rest configs
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
    # Iceberg Table
    tbl = catalog.load_table(("silver", "br_setex_bot_isic_cuci"))
    tbl.append(arrow_table)
    return


if __name__ == "__main__":
    print("Rodando")
    Raw_table = load_from_land(
                    schema=r"bronze/trade_balance", 
                    file_name="ISIC_BR_Data.xlsx",
                    sheet="DADOS_ISIC_CUCI",
                    now=datetime.today().replace(second=0)
                    )
    Arrow_table=pandas_to_arrow(Raw_table)
    up_till_iceberg(Arrow_table)