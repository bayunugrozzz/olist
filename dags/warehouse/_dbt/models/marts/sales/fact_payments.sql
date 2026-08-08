with payments as (
    select * from {{ ref('stg_order_payments') }}
),

orders as (
    select order_id, customer_id, order_purchase_at, order_status from {{ ref('stg_orders') }}
),

dim_customers as (
    select customer_key, customer_id from {{ ref('dim_customers') }}
),

final as (
    select
        -- Primary / Composite Key
        {{ dbt_utils.generate_surrogate_key(['payments.order_id', 'payments.payment_sequential']) }} as payment_key,

        -- Degenerate Dimensions (IDs & Sequence)
        payments.order_id,
        payments.payment_sequential,

        -- Foreign Key
        dim_customers.customer_key,

        -- Payment Attributes
        payments.payment_type,
        orders.order_status,

        -- Timestamps
        orders.order_purchase_at,

        -- Metrics / Measures
        payments.payment_installments,
        payments.payment_value,

        -- Flags
        case 
            when payments.payment_installments > 1 then true 
            else false 
        end as is_installment,

        case 
            when payments.payment_type = 'voucher' then true 
            else false 
        end as is_voucher_used

    from payments
    inner join orders 
        on payments.order_id = orders.order_id
    left join dim_customers 
        on orders.customer_id = dim_customers.customer_id
)

select * from final