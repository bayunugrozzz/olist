with reviews as (
    select * from {{ ref('stg_order_reviews') }}
),

orders as (
    select order_id, customer_id, order_purchase_at, order_status from {{ ref('stg_orders') }}
),

-- Mengambil produk & seller utama per order untuk koneksi ke dimensi
order_items_summary as (
    select
        order_id,
        max(product_id) as product_id,
        max(seller_id) as seller_id,
        count(order_item_id) as total_items_in_order
    from {{ ref('stg_order_items') }}
    group by order_id
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
        -- Primary Key (Surrogate Key)
        {{ dbt_utils.generate_surrogate_key(['reviews.review_id', 'reviews.order_id']) }} as review_key,

        -- Degenerate Dimensions (IDs)
        reviews.review_id,
        reviews.order_id,

        -- Foreign Keys
        dim_customers.customer_key,
        dim_products.product_key,
        dim_sellers.seller_key,

        -- Review Metrics & Attributes
        reviews.review_score,
        reviews.review_comment_title,
        reviews.review_comment_message,

        -- Timestamps
        reviews.review_created_at,
        reviews.review_answered_at,
        orders.order_purchase_at,

        -- Operational Response Time (Kalkulasi Jam)
        extract(epoch from (reviews.review_answered_at - reviews.review_created_at)) / 3600.0 as review_response_time_hours,

        -- Flags
        case when reviews.review_score >= 4 then true else false end as is_satisfied,
        case when reviews.review_comment_message is not null then true else false end as has_comment

    from reviews
    inner join orders 
        on reviews.order_id = orders.order_id
    left join order_items_summary 
        on reviews.order_id = order_items_summary.order_id
    left join dim_customers 
        on orders.customer_id = dim_customers.customer_id
    left join dim_products 
        on order_items_summary.product_id = dim_products.product_id
    left join dim_sellers 
        on order_items_summary.seller_id = dim_sellers.seller_id
)

select * from final