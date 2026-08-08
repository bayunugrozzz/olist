with source as (
    select * from {{ source('staging', 'sellers') }}
),

renamed as (
    select
        -- Primary Key
        cast(seller_id as text) as seller_id,

        -- Location
        cast(seller_zip_code_prefix as text) as seller_zip_code_prefix,
        initcap(trim(seller_city)) as seller_city,
        upper(trim(seller_state)) as seller_state

    from source
)

select * from renamed