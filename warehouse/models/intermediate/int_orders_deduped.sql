-- ONE row per order: its latest version.
-- Handles both exact duplicate rows AND late updates (a refund re-sent days later
-- in another file): the newest updated_at wins, then the newest file.
with ranked as (
    select *,
        row_number() over (
            partition by order_id
            order by updated_at desc nulls last, _file_date desc, _file_row desc
        )                                                   as version_rank,
        count(*) over (partition by order_id)               as rows_received
    from {{ ref('stg_orders') }}
    where order_id is not null
)

select * from ranked
where version_rank = 1
