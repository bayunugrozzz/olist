{% snapshot snap_closed_deals %}

{{
    config(
      target_schema='snapshots',
      unique_key='mql_id',
      strategy='check',
      check_cols=[
        'seller_id', 
        'sdr_id', 
        'sr_id', 
        'won_date', 
        'business_segment', 
        'lead_type', 
        'lead_behaviour_profile', 
        'has_company', 
        'has_gtin', 
        'average_stock', 
        'business_type', 
        'declared_product_catalog_size', 
        'declared_monthly_revenue'
      ],
      invalidate_hard_deletes=True
    )
}}

select
    mql_id,
    seller_id,
    sdr_id,
    sr_id,
    won_date,
    business_segment,
    lead_type,
    lead_behaviour_profile,
    has_company,
    has_gtin,
    average_stock,
    business_type,
    declared_product_catalog_size,
    declared_monthly_revenue
from {{ source('staging', 'closed_deals') }}

{% endsnapshot %}