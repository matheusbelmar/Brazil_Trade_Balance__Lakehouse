{{ config(
    materialized='table',
    schema='gold'
) }}

select *
from {{ source('silver', 'br_setex_tb_ISIC_CUCI') }}