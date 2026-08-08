with leads as (
    select * from {{ ref('stg_marketing_qualified_leads') }}
),

closed_deals as (
    select * from {{ ref('stg_closed_deals') }}
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['leads.mql_id']) }} as mql_key,

        -- Natural Keys
        leads.mql_id,
        closed_deals.seller_id,
        closed_deals.sdr_id,
        closed_deals.sr_id,

        -- Acquisition Attributes
        coalesce(leads.landing_page_id, 'unknown') as landing_page_id,
        coalesce(leads.origin, 'unknown') as origin,

        -- Business Lead Profile
        coalesce(closed_deals.business_segment, 'unknown') as business_segment,
        coalesce(closed_deals.lead_type, 'unknown') as lead_type,
        coalesce(closed_deals.lead_behaviour_profile, 'unknown') as lead_behaviour_profile,
        coalesce(closed_deals.business_type, 'unknown') as business_type,
        
        -- Flags
        coalesce(closed_deals.has_company, false) as has_company,
        coalesce(closed_deals.has_gtin, false) as has_gtin,
        case when closed_deals.seller_id is not null then true else false end as is_won

    from leads
    left join closed_deals
        on leads.mql_id = closed_deals.mql_id
)

select * from final