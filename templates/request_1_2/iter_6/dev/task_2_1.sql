with v1 as (
SELECT
    to_date(createtime) as createdate
    ,goodsid
    ,count(distinct playerid) as total_players
    ,count(*) as total_orders
    ,sum(realmoney) as total_amount
    ,lag(sum(realmoney)) over(PARTITION BY goodsid ORDER BY to_date(createtime)) as last_total_amount
    ,count(*) / SUM(COUNT(*)) over(PARTITION BY to_date(createtime)) as total_orders_percentage
    ,sum(realmoney) / SUM(sum(realmoney)) over(PARTITION BY to_date(createtime)) as total_amount_percentage
    ,row_number() over(PARTITION BY to_date(createtime) ORDER BY sum(realmoney) desc) as rk
FROM
    (
        select 
            createtime
            ,realmoney
            ,playerid
            ,goodsid
        from 
            {db_read}.ods_order_test
        where
            dt >= '{start_dt}' and dt < '{end_dt}'
        AND status = {status}
    ) as t1
GROUP BY
    to_date(createtime), goodsid
ORDER BY
    createdate, goodsid
)

select 
    t1.createdate as `日期`
    ,t1.goodsid as `商品id`
    ,t2.goods_name as `商品名称`
    ,t1.total_players as `总付费人数`
    ,t1.total_orders as `总订单数`
    ,t1.total_amount as `总金额`
    ,t1.rk as `充值金额排名`
    ,t1.last_total_amount as `上一日总金额`
    ,t1.total_amount - t1.last_total_amount as `日环比`
    ,CONCAT(FORMAT_NUMBER(CAST(t1.total_orders_percentage AS DECIMAL(17, 15)) * 100, 1), '%') as `订单占比`
    ,CONCAT(FORMAT_NUMBER(CAST(t1.total_amount_percentage AS DECIMAL(17, 15)) * 100, 1), '%') as `金额占比`   
from
    v1 as t1
LEFT JOIN
    {db_read}.ods_config_shop_test as t2
on
    t1.goodsid = cast(t2.goods_id as string)
    and t2.game_id = '1000'
where
    rk <= 20
order BY
    `日期` desc, `商品id`