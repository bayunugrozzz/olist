with leads as (
    select * from {{ ref('stg_marketing_qualified_leads') }}
),

closed_deals as (
    select * from {{ ref('stg_closed_deals') }}
),

dim_marketing_leads as (
    select mql_key, mql_id from {{ ref('dim_marketing_leads') }}
),

dim_sellers as (
    select seller_key, seller_id from {{ ref('dim_sellers') }}
),

-- FIX 1: Menggunakan seller_key karena fact_sales menyimpan surrogate key
seller_performance as (
    select
        seller_key,
        min(order_purchase_at) as first_sale_at,
        count(distinct order_id) as total_orders_completed,
        sum(price) as total_gross_revenue
    from {{ ref('fact_sales') }}
    group by seller_key
),

final as (
    select
        -- Primary Key (Surrogate Key)
        {{ dbt_utils.generate_surrogate_key(['leads.mql_id']) }} as funnel_key,

        -- Foreign Keys
        dim_marketing_leads.mql_key,
        dim_sellers.seller_key,

        -- Natural Keys
        leads.mql_id,
        closed_deals.seller_id,
        closed_deals.sdr_id,
        closed_deals.sr_id,

        -- Timestamps
        leads.first_contact_at,
        closed_deals.won_at,
        seller_performance.first_sale_at,

        -- Conversion Metrics (Durasi dalam Hari)
        extract(day from (closed_deals.won_at - leads.first_contact_at)) as days_to_close,
        extract(day from (seller_performance.first_sale_at - closed_deals.won_at)) as days_to_first_sale,

        -- Declared / Projected Metrics (Estimasi Seller)
        closed_deals.declared_product_catalog_size,
        closed_deals.declared_monthly_revenue,

        -- Actual Realized Metrics (Performa Riil)
        coalesce(seller_performance.total_orders_completed, 0) as total_orders_completed,
        coalesce(seller_performance.total_gross_revenue, 0.00) as total_gross_revenue,

        -- Flags
        case when closed_deals.seller_id is not null then true else false end as is_won,
        case when seller_performance.total_orders_completed > 0 then true else false end as is_active_seller

    from leads
    left join closed_deals 
        on leads.mql_id = closed_deals.mql_id
    left join dim_marketing_leads 
        on leads.mql_id = dim_marketing_leads.mql_id
    left join dim_sellers 
        on closed_deals.seller_id = dim_sellers.seller_id
    -- FIX 2: Join seller_performance menggunakan dim_sellers.seller_key
    left join seller_performance 
        on dim_sellers.seller_key = seller_performance.seller_key
)

select * from final