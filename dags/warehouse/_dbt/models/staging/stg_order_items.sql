with source as (
    select * from {{ source('staging', 'order_items') }}
),

renamed as (
    select
        -- Primary & Foreign Keys
        cast(order_id as text) as order_id,
        cast(order_item_id as integer) as order_item_id,
        cast(product_id as text) as product_id,
        cast(seller_id as text) as seller_id,

        -- Timestamps
        cast(shipping_limit_date as timestamp) as shipping_limit_at,

        -- Metrics / Amounts
        cast(price as numeric(10, 2)) as price,
        cast(freight_value as numeric(10, 2)) as freight_value

    from source
)

select * from renamed