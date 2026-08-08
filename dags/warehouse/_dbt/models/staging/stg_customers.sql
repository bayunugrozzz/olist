with source as (
    select * from {{ source('staging', 'customers') }}
),

renamed as (
    select
        -- Primary & Composite Keys
        cast(customer_id as text) as customer_id,
        cast(customer_unique_id as text) as customer_unique_id,

        -- Location
        cast(customer_zip_code_prefix as text) as customer_zip_code_prefix,
        initcap(trim(customer_city)) as customer_city,
        upper(trim(customer_state)) as customer_state

    from source
)

select * from renamed