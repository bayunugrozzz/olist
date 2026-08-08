with sellers as (
    select * from {{ ref('stg_sellers') }}
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_key,

        -- Natural Key
        seller_id,

        -- Location Attributes
        coalesce(seller_zip_code_prefix, 'unknown') as seller_zip_code_prefix,
        coalesce(seller_city, 'unknown') as seller_city,
        coalesce(seller_state, 'unknown') as seller_state

    from sellers
)

select * from final