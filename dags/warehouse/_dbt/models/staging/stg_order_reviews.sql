with source as (
    select * from {{ source('staging', 'order_reviews') }}
),

renamed as (
    select
        -- Primary & Foreign Keys
        cast(review_id as text) as review_id,
        cast(order_id as text) as order_id,

        -- Ratings & Text
        cast(review_score as integer) as review_score,
        trim(review_comment_title) as review_comment_title,
        trim(review_comment_message) as review_comment_message,

        -- Timestamps
        cast(review_creation_date as timestamp) as review_created_at,
        cast(review_answer_timestamp as timestamp) as review_answered_at

    from source
)

select * from renamed