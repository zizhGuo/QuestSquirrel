WITH v1 as (
    SELECT
        t1.dt as createdate
        ,t2.vip
        ,MAX(t1.realmoney) as max_price
        ,MIN(t1.realmoney) as min_price
    from
        b1_statistics.ods_order_test as t1
    left join
        b1_statistics.ods_log_order_test as t2
    on
        t1.platformorderid  = t2.platformorderid 
    WHERE
        t1.dt  >= '{start_dt}' and t1.dt < '{end_dt}' 
        AND t1.status = {status}
        and t2.step = {step}
    GROUP BY
        t1.dt, t2.vip
),

v2 as (
    select
        dt as createdate
        ,1 as k
        ,count(distinct playerid) as total_players
        ,count(*) as total_records
        ,sum(realmoney) as total_amount
    from
        b1_statistics.ods_order_test
    WHERE
        dt  >= '{start_dt}' and dt < '{end_dt}' 
        AND status = {status}
    group by
        dt
),

v3 as (
SELECT
    t1.createdate as createdate
    ,t1.vip
    ,COUNT(distinct t1.playerid) as vip_total_players
    ,COUNT(*) as vip_total_records
    ,SUM(t1.realmoney) as vip_total_amount
    ,COUNT(*) / SUM(COUNT(*)) over(partition by t1.createdate) as total_records_percentage
    ,MAX(t1.realmoney) as max_price
    ,COUNT(distinct if(t1.realmoney = t2.max_price, t1.playerid, NULL)) as max_price_players
    ,MIN(t1.realmoney) as min_price
    ,COUNT(distinct if(t1.realmoney = t2.min_price, t1.playerid, NULL)) as min_price_players
    ,1 as k
FROM
    (select
        t1.dt as createdate
        ,t2.vip
        ,t1.playerid
        ,t1.realmoney
    from
        b1_statistics.ods_order_test as t1
    left join
        b1_statistics.ods_log_order_test as t2
    on
        t1.platformorderid  = t2.platformorderid 
    WHERE
        t1.dt  >= '{start_dt}' and t1.dt < '{end_dt}' 
        AND t1.status = {status}
        and t2.step = {step}
        ) as t1
LEFT JOIN
    v1 as t2
ON
    t1.createdate = t2.createdate and t1.vip = t2.vip
GROUP BY
    t1.createdate, t1.vip
)

select 
    CONCAT(SUBSTRING(t1.createdate, 1, 4), '-', SUBSTRING(t1.createdate, 5, 2), '-', SUBSTRING(t1.createdate, 7, 2)) as `日期`
    ,if(t2.total_players is null, 0, t2.total_players) as `总充值人数`
    ,if(t2.total_records is null, 0, t2.total_records) as `总充值笔数`
    ,if(t2.total_amount is null, 0, t2.total_amount) as `总充值金额`
    ,t1.vip as `VIP等级`
    ,t1.vip_total_players as `VIP充值人数`
    ,t1.vip_total_records as `VIP充值笔数`
    ,t1.vip_total_amount as `VIP充值金额`
    ,CONCAT(FORMAT_NUMBER(CAST(t1.total_records_percentage AS DECIMAL(17, 15)) * 100, 1), '%') as `订单占比`
    ,t1.max_price as `充值最高档位`
    ,t1.max_price_players as `充值最高档位人数`
    ,t1.min_price as `充值最低档位`
    ,t1.min_price_players as `充值最低档位人数`
from
    v3 as t1
LEFT JOIN
    v2 as t2
ON
    t1.createdate = t2.createdate and t1.k = t2.k
order by
    `日期` desc, `VIP等级`