with v1 as (
    select
        t1.playerid
        ,t1.createtime
        ,t1.realmoney
        ,t1.goodsid
        ,row_number() over(partition by date(t1.createtime), t1.playerid order by t1.createtime desc) as rk
    from
        b1_statistics.ods_order_test as t1
    left join
        b1_statistics.ods_log_order_test as t2
    on
        t1.platformorderid  = t2.platformorderid 
    WHERE
        t1.dt >='{pre_start_dt}' and t1.dt < '{end_dt}'
        AND t1.status = {status}
        and t2.step = {step}
)
,
v2 as (
    select
        t1.playerid
        ,t1.createtime
        ,date(t1.createtime) as cur_purchase_date
        ,t1.realmoney
        ,t1.goodsid
        ,lag(t1.createtime) over(partition by t1.playerid order by t1.createtime) as last_purchase
        ,date(lag(t1.createtime) over(partition by t1.playerid order by t1.createtime)) as last_purchase_date
        ,datediff(date(t1.createtime), date(lag(t1.createtime) over(partition by t1.playerid order by t1.createtime))) as date_diff
        
    from
        v1 as t1
    WHERE
        t1.rk = 1
    order by
        t1.createtime desc
),
v3 as (
    select
        t1.playerid
        ,t2.createtime as account_createtime
        ,t1.createtime as order_createtime
        ,t1.cur_purchase_date
        ,t1.last_purchase
        ,t1.last_purchase_date
        ,t1.date_diff
        ,case
            when t1.date_diff is null and t2.createtime >= '{pre_start_date}' then 1
            when t1.date_diff = 1 then 2
            when t1.date_diff = 2 then 3
            when t1.date_diff = 3 then 4
            when t1.date_diff = 4 then 5
            when t1.date_diff = 5 then 6
            when t1.date_diff = 6 then 7
            when t1.date_diff = 7 then 8
            when t1.date_diff <= 14 then 9
            when t1.date_diff <= 30 then 10
            when t1.date_diff <= 90 then 11
            when t1.date_diff <= 180 then 12
            else 13 end as rk
        ,case
            when t1.date_diff is null and t2.createtime >= '{pre_start_date}' then '0天'
            when t1.date_diff = 1 then '1天'
            when t1.date_diff = 2 then '2天'
            when t1.date_diff = 3 then '3天'
            when t1.date_diff = 4 then '4天'
            when t1.date_diff = 5 then '5天'
            when t1.date_diff = 6 then '6天'
            when t1.date_diff = 7 then '7天'
            when t1.date_diff <= 14 then '14天'
            when t1.date_diff <= 30 then '30天'
            when t1.date_diff <= 90 then '30天以上到90天内'
            when t1.date_diff <= 180 then '90天以上到180天内'
            else '180天以上或其他' end as rechatge_interval
        ,t1.realmoney
        ,t1.goodsid
    from
        v2 as t1
    left join
        b1_statistics.ods_snapshot_player_test as t2
    on
        t1.playerid = t2.playerid
)

select
    t1.`日期`
    ,t1.`今日充值用户数`
    ,t1.`充值间隔时间`
    ,t1.`充值人数`
    ,t1.`充值订单数`
    ,t1.`充值金额`
    ,t1.`充值人数占比`
from
    (
    select
        cur_purchase_date as `日期`
        ,sum(count(distinct playerid)) over(partition by cur_purchase_date) as `今日充值用户数`
        ,rk as `序号`
        ,rechatge_interval as `充值间隔时间`
        ,count(distinct playerid) as `充值人数`
        ,count(*) as `充值订单数`
        ,sum(realmoney) as `充值金额`
        ,CONCAT(FORMAT_NUMBER(CAST(count(distinct playerid) / sum(count(distinct playerid)) over(partition by cur_purchase_date) AS DECIMAL(17, 15)) * 100, 1), '%') as `充值人数占比`
    from 
        v3
    where
        cur_purchase_date >= '{start_date}'  and cur_purchase_date < '{end_date}'
    group by
        cur_purchase_date, rk, rechatge_interval
    order by
        cur_purchase_date desc, rk, rechatge_interval asc
    ) as t1
order by t1.`日期` desc
