with products as (
    select * from {{ ref('stg_products') }}
),

category_translation as (
    select * from {{ ref('stg_product_category_name') }}
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['products.product_id']) }} as product_key,

        -- Natural Key
        products.product_id,

        -- Categories (Portuguese & English Translation)
        coalesce(products.product_category_name, 'nao_informado') as product_category_name_pt,
        coalesce(category_translation.product_category_name_english, 'others/unspecified') as product_category_name,

        -- Product Attributes
        coalesce(products.product_name_length, 0) as product_name_length,
        coalesce(products.product_description_length, 0) as product_description_length,
        coalesce(products.product_photos_qty, 0) as product_photos_qty,

        -- Physical Specs
        coalesce(products.product_weight_g, 0) as product_weight_g,
        coalesce(products.product_length_cm, 0) as product_length_cm,
        coalesce(products.product_height_cm, 0) as product_height_cm,
        coalesce(products.product_width_cm, 0) as product_width_cm,
        
        -- Calculated Field: Physical Volume (cm3)
        (
            coalesce(products.product_length_cm, 0) * 
            coalesce(products.product_height_cm, 0) * 
            coalesce(products.product_width_cm, 0)
        ) as product_volume_cm3

    from products
    left join category_translation 
        on products.product_category_name = category_translation.product_category_name
)

select * from final