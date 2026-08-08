with source as (
    select * from {{ source('staging', 'closed_deals') }}
),

renamed as (
    select
        -- Primary & Foreign Keys
        cast(mql_id as text) as mql_id,
        cast(seller_id as text) as seller_id,
        cast(sdr_id as text) as sdr_id,
        cast(sr_id as text) as sr_id,

        -- Timestamps
        cast(won_date as timestamp) as won_at,

        -- Business Attributes
        lower(trim(business_segment)) as business_segment,
        lower(trim(lead_type)) as lead_type,
        lower(trim(lead_behaviour_profile)) as lead_behaviour_profile,
        lower(trim(business_type)) as business_type,

        -- Flags
        cast(has_company as boolean) as has_company,
        cast(has_gtin as boolean) as has_gtin,

        -- Declared Metrics
        cast(average_stock as text) as average_stock,
        cast(declared_product_catalog_size as numeric(10, 2)) as declared_product_catalog_size,
        cast(declared_monthly_revenue as numeric(10, 2)) as declared_monthly_revenue

    from source
)

select * from renamed