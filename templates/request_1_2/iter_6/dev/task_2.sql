select
    createdate as `日期`
    ,amount_level as `付费金额区间`
    ,total_amount as `总付费金额`
    ,total_players as `总付费人数`
    ,total_players_percentage as `总付费人数占比`
    ,total_orders as `总订单数`
    ,total_orders_percentage as `总订单数占比`
    ,sum_charge_percentage as `总付费金额占比`
from
(
    SELECT
        to_date(t1.createtime) as createdate
        ,t1.realmoney as amount_level
        ,sum(t1.realmoney) as total_amount
        ,count(distinct t1.playerid) as total_players
        ,CONCAT(FORMAT_NUMBER(CAST(count(distinct t1.playerid) / sum(count(distinct t1.playerid)) over(partition by to_date(t1.createtime)) AS DECIMAL(17, 15)) * 100, 1), '%') as total_players_percentage
        ,count(*) as total_orders
        ,CONCAT(FORMAT_NUMBER(CAST(COUNT(*) / SUM(COUNT(*)) over(partition by to_date(t1.createtime)) AS DECIMAL(17, 15)) * 100, 1), '%') as total_orders_percentage
        ,CONCAT(FORMAT_NUMBER(CAST(sum(t1.realmoney) / sum(sum(t1.realmoney)) over(partition by to_date(t1.createtime)) AS DECIMAL(17, 15)) * 100, 1), '%') as sum_charge_percentage
    from
        (
          select 
            createtime
            ,realmoney
            ,playerid
          from 
            {db_read}.ods_order_test
          where
            dt  >= '{start_dt}' and dt < '{end_dt}'
            AND status = {status}
        ) as t1
    group by
        to_date(t1.createtime), t1.realmoney
    order by
        to_date(t1.createtime), t1.realmoney
) as t2
order by
  `日期` desc, `付费金额区间` asc