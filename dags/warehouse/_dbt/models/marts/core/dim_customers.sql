with customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
        
        -- Natural Keys
        customer_id,
        customer_unique_id,

        -- Location Attributes
        coalesce(customer_zip_code_prefix, 'unknown') as customer_zip_code_prefix,
        coalesce(customer_city, 'unknown') as customer_city,
        coalesce(customer_state, 'unknown') as customer_state

    from customers
)

select * from final
