with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

dim_customers as (
    select customer_key, customer_id from {{ ref('dim_customers') }}
),

dim_products as (
    select product_key, product_id from {{ ref('dim_products') }}
),

dim_sellers as (
    select seller_key, seller_id from {{ ref('dim_sellers') }}
),

final as (
    select
        -- Primary / Composite Key
        {{ dbt_utils.generate_surrogate_key(['order_items.order_id', 'order_items.order_item_id']) }} as sales_item_key,
        
        -- Degenerate Dimensions (IDs)
        order_items.order_id,
        order_items.order_item_id,

        -- Foreign Keys (Menghubungkan ke Conformed Dimensions)
        dim_customers.customer_key,
        dim_products.product_key,
        dim_sellers.seller_key,

        -- Transaction Attributes
        orders.order_status,

        -- Timestamps
        orders.order_purchase_at,
        orders.order_approved_at,
        orders.order_delivered_carrier_at,
        orders.order_delivered_customer_at,
        orders.order_estimated_delivery_at,
        order_items.shipping_limit_at,

        -- Metrics / Financial Measures
        order_items.price,
        order_items.freight_value,
        (order_items.price + order_items.freight_value) as total_item_amount,

        -- Operational Metrics & SLA (Kalkulasi Durasi dalam Hari)
        extract(day from (orders.order_delivered_customer_at - orders.order_purchase_at)) as delivery_duration_days,
        extract(day from (orders.order_estimated_delivery_at - orders.order_delivered_customer_at)) as delivery_delay_vs_estimated_days,
        
        -- Flags
        case 
            when orders.order_delivered_customer_at > orders.order_estimated_delivery_at then true 
            else false 
        end as is_late_delivery

    from order_items
    inner join orders 
        on order_items.order_id = orders.order_id
    left join dim_customers 
        on orders.customer_id = dim_customers.customer_id
    left join dim_products 
        on order_items.product_id = dim_products.product_id
    left join dim_sellers 
        on order_items.seller_id = dim_sellers.seller_id
)

select * from final