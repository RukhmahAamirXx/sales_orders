with sales as (
    select * from SALES_DB.RAW.SALES_DATA
),

products as (
    -- This pulls from your CSV in dbt seeds
    select * from {{ ref('products') }}
),

joined as (
    select
        s.customer_id,
        p.product_category,
        (s.quantity * p.price) as line_item_total
    from sales s
    inner join products p
        on s.product_id = p.product_id
),

final as (
    select
        customer_id,
        product_category,
        sum(line_item_total) as total_sales_amount
    from joined
    group by 1, 2
)

-- Added ORDER BY to make "Per Customer Per Category" clear
select * from final 
order by customer_id, product_category