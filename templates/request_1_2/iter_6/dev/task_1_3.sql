with v1 as (
    select
        t1.playerid
        ,t1.createtime
        ,t1.realmoney
        ,t1.goodsid
        ,row_number() over(partition by to_date(t1.createtime), t1.playerid order by t1.createtime desc) as rk
    from
        b1_statistics.ods_order_test as t1
    left join
        b1_statistics.ods_log_order_test as t2
    on
        t1.platformorderid  = t2.platformorderid 
    WHERE
        t1.dt >= '{pre_start_dt}' and t1.dt < '{end_dt}'
        AND t1.status = {status}
        and t2.step = {step}
)
,
v2 as (
    select
        t1.playerid
        ,t1.createtime
        ,to_date(t1.createtime) as cur_purchase_date
        ,t1.realmoney
        ,t1.goodsid
        ,lag(t1.createtime) over(partition by t1.playerid order by t1.createtime) as last_purchase
        ,to_date(lag(t1.createtime) over(partition by t1.playerid order by t1.createtime)) as last_purchase_date
        ,datediff(to_date(t1.createtime), to_date(lag(t1.createtime) over(partition by t1.playerid order by t1.createtime))) as date_diff
        
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
    *
from (
    select
        cur_purchase_date as `日期`
        ,goodsid as `商品ID`
        ,goods_name as `商品名称`
        ,count(distinct playerid) as `充值人数`
        ,sum(realmoney) as `充值金额`
        ,row_number() over(partition by cur_purchase_date order by sum(realmoney) desc) as `充值金额排名`
        ,count(distinct if(rechatge_interval = '0天', playerid, null)) as `间隔0天充值人数`
        ,sum(if(rechatge_interval = '0天', realmoney, 0)) as `间隔0天充值金额`
        ,count(distinct if(rechatge_interval = '1天', playerid, null)) as `间隔1天充值人数`
        ,sum(if(rechatge_interval = '1天', realmoney, 0)) as `间隔1天充值金额`
        ,count(distinct if(rechatge_interval = '2天', playerid, null)) as `间隔2天充值人数`
        ,sum(if(rechatge_interval = '2天', realmoney, 0)) as `间隔2天充值金额`
        ,count(distinct if(rechatge_interval = '3天', playerid, null)) as `间隔3天充值人数`
        ,sum(if(rechatge_interval = '3天', realmoney, 0)) as `间隔3天充值金额`
        ,count(distinct if(rechatge_interval = '4天', playerid, null)) as `间隔4天充值人数`
        ,sum(if(rechatge_interval = '4天', realmoney, 0)) as `间隔4天充值金额`
        ,count(distinct if(rechatge_interval = '5天', playerid, null)) as `间隔5天充值人数`
        ,sum(if(rechatge_interval = '5天', realmoney, 0)) as `间隔5天充值金额`
        ,count(distinct if(rechatge_interval = '6天', playerid, null)) as `间隔6天充值人数`
        ,sum(if(rechatge_interval = '6天', realmoney, 0)) as `间隔6天充值金额`
        ,count(distinct if(rechatge_interval = '7天', playerid, null)) as `间隔7天充值人数`
        ,sum(if(rechatge_interval = '7天', realmoney, 0)) as `间隔7天充值金额`
        ,count(distinct if(rechatge_interval = '14天', playerid, null)) as `间隔14天充值人数`
        ,sum(if(rechatge_interval = '14天', realmoney, 0)) as `间隔14天充值金额`
        ,count(distinct if(rechatge_interval = '30天', playerid, null)) as `间隔30天充值人数`
        ,sum(if(rechatge_interval = '30天', realmoney, 0)) as `间隔30天充值金额`
        ,count(distinct if(rechatge_interval = '30天以上到90天内', playerid, null)) as `间隔30天以上到90天内充值人数`
        ,sum(if(rechatge_interval = '30天以上到90天内', realmoney, 0)) as `间隔30天以上到90天内充值金额`
        ,count(distinct if(rechatge_interval = '90天以上到180天内', playerid, null)) as `间隔90天以上到180天内充值人数`
        ,sum(if(rechatge_interval = '90天以上到180天内', realmoney, 0)) as `间隔90天以上到180天内充值金额`
        ,count(distinct if(rechatge_interval = '180天以上或其他', playerid, null)) as `间隔180天以上充值人数`
        ,sum(if(rechatge_interval = '180天以上或其他', realmoney, 0)) as `间隔180天以上充值金额`
    from
        v3 as t1
    LEFT JOIN
        b1_statistics.ods_config_shop_test as t2
    on
        goodsid = cast(goods_id as string)
        and t2.game_id = '1000'
    where
        cur_purchase_date >= '{start_date}' and cur_purchase_date < '{end_date}'
    group by
        cur_purchase_date, goodsid, goods_name
) as t1
where
    `充值金额排名` <= 20
order by
    `日期` desc,  `充值金额` desc