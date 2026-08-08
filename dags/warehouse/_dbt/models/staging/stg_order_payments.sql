with source as (
    select * from {{ source('staging', 'order_payments') }}
),

renamed as (
    select
        -- Foreign Keys & Sequence
        cast(order_id as text) as order_id,
        cast(payment_sequential as integer) as payment_sequential,

        -- Attributes
        lower(trim(payment_type)) as payment_type,

        -- Metrics
        cast(payment_installments as integer) as payment_installments,
        cast(payment_value as numeric(10, 2)) as payment_value

    from source
)

select * from renamed