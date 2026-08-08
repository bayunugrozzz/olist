with source as (
    select * from {{ source('staging', 'products') }}
),

renamed as (
    select
        -- Primary Key
        cast(product_id as text) as product_id,

        -- Category
        lower(trim(product_category_name)) as product_category_name,

        -- Attributes
        cast(product_name_lenght as integer) as product_name_length,
        cast(product_description_lenght as integer) as product_description_length,
        cast(product_photos_qty as integer) as product_photos_qty,

        -- Dimensions & Physical Specs
        cast(product_weight_g as numeric(10, 2)) as product_weight_g,
        cast(product_length_cm as numeric(10, 2)) as product_length_cm,
        cast(product_height_cm as numeric(10, 2)) as product_height_cm,
        cast(product_width_cm as numeric(10, 2)) as product_width_cm

    from source
)

select * from renamed