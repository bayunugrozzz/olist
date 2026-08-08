with source as (
    select * from {{ source('staging', 'marketing_qualified_leads') }}
),

renamed as (
    select
        -- Primary Key
        cast(mql_id as text) as mql_id,

        -- Attributes
        cast(landing_page_id as text) as landing_page_id,
        lower(trim(origin)) as origin,

        -- Timestamps
        cast(first_contact_date as timestamp) as first_contact_at

    from source
)

select * from renamed