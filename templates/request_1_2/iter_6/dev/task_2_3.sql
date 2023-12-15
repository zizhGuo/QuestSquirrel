WITH A AS (
    SELECT
        CONCAT(SUBSTRING(t1.dt, 1, 4), '-', SUBSTRING(t1.dt, 5, 2), '-', SUBSTRING(t1.dt, 7, 2)) as `日期`
        ,t1.goodsid as `商品id`
        ,t3.goods_name  as `商品名称`
        ,count(distinct t1.playerid) as `充值人数`
        ,count(*) as `充值订单数`
        ,sum(t1.realmoney) as `充值金额`
        ,row_number() OVER(PARTITION BY t1.dt ORDER BY sum(t1.realmoney) DESC) as `充值金额排名`
        ,count(if(t2.vip = 0, t1.playerid, null)) as `VIP0充值人数`
        ,sum(if(t2.vip = 0, 1, 0)) as `VIP0充值订单数`
        ,sum(if(t2.vip = 0, t1.realmoney, 0)) as `VIP0充值金额`
        ,CONCAT(FORMAT_NUMBER(CAST(sum(if(t2.vip = 0, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `VIP0订单占比`

        ,count(if(t2.vip = 1, t1.playerid, null)) as `VIP1充值人数`
        ,sum(if(t2.vip = 1, 1, 0)) as `VIP1充值订单数`
        ,sum(if(t2.vip = 1, t1.realmoney, 0)) as `VIP1充值金额`
        ,CONCAT(FORMAT_NUMBER(CAST(sum(if(t2.vip = 1, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `VIP1订单占比`

        ,count(if(t2.vip = 2, t1.playerid, null)) as `VIP2充值人数`
        ,sum(if(t2.vip = 2, 1, 0)) as `VIP2充值订单数`
        ,sum(if(t2.vip = 2, t1.realmoney, 0)) as `VIP2充值金额`
        ,CONCAT(FORMAT_NUMBER(CAST(sum(if(t2.vip = 2, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `VIP2订单占比`

        ,count(if(t2.vip = 3, t1.playerid, null)) as `VIP3充值人数`
        ,sum(if(t2.vip = 3, 1, 0)) as `VIP3充值订单数`
        ,sum(if(t2.vip = 3, t1.realmoney, 0)) as `VIP3充值金额`
        ,CONCAT(FORMAT_NUMBER(CAST(sum(if(t2.vip = 3, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `VIP3订单占比`

        ,count(if(t2.vip = 4, t1.playerid, null)) as `VIP4充值人数`
        ,sum(if(t2.vip = 4, 1, 0)) as `VIP4充值订单数`
        ,sum(if(t2.vip = 4, t1.realmoney, 0)) as `VIP4充值金额`
        ,CONCAT(FORMAT_NUMBER(CAST(sum(if(t2.vip = 4, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `VIP4订单占比`

        ,count(if(t2.vip = 5, t1.playerid, null)) as `VIP5充值人数`
        ,sum(if(t2.vip = 5, 1, 0)) as `VIP5充值订单数`
        ,sum(if(t2.vip = 5, t1.realmoney, 0)) as `VIP5充值金额`
        ,CONCAT(FORMAT_NUMBER(CAST(sum(if(t2.vip = 5, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `VIP5订单占比`

        ,count(if(t2.vip = 6, t1.playerid, null)) as `VIP6充值人数`
        ,sum(if(t2.vip = 6, 1, 0)) as `VIP6充值订单数`
        ,sum(if(t2.vip = 6, t1.realmoney, 0)) as `VIP6充值金额`
        ,CONCAT(FORMAT_NUMBER(CAST(sum(if(t2.vip = 6, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `VIP6订单占比`

        ,count(if(t2.vip = 7, t1.playerid, null)) as `VIP7充值人数`
        ,sum(if(t2.vip = 7, 1, 0)) as `VIP7充值订单数`
        ,sum(if(t2.vip = 7, t1.realmoney, 0)) as `VIP7充值金额`
        ,CONCAT(FORMAT_NUMBER(CAST(sum(if(t2.vip = 7, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `VIP7订单占比`

        ,count(if(t2.vip = 8, t1.playerid, null)) as `VIP8充值人数`
        ,sum(if(t2.vip = 8, 1, 0)) as `VIP8充值订单数`
        ,sum(if(t2.vip = 8, t1.realmoney, 0)) as `VIP8充值金额`
        ,CONCAT(FORMAT_NUMBER(CAST(sum(if(t2.vip = 8, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `VIP8订单占比`

        ,count(if(t2.vip = 9, t1.playerid, null)) as `VIP9充值人数`
        ,sum(if(t2.vip = 9, 1, 0)) as `VIP9充值订单数`
        ,sum(if(t2.vip = 9, t1.realmoney, 0)) as `VIP9充值金额`
        ,CONCAT(FORMAT_NUMBER(CAST(sum(if(t2.vip = 9, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `VIP9订单占比`

        ,count(if(t2.vip = 10, t1.playerid, null)) as `VIP10充值人数`
        ,sum(if(t2.vip = 10, 1, 0)) as `VIP10充值订单数`
        ,sum(if(t2.vip = 10, t1.realmoney, 0)) as `VIP10充值金额`
        ,CONCAT(FORMAT_NUMBER(CAST(sum(if(t2.vip = 10, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `VIP10订单占比`

        ,count(if(t2.vip = 11, t1.playerid, null)) as `VIP11充值人数`
        ,sum(if(t2.vip = 11, 1, 0)) as `VIP11充值订单数`
        ,sum(if(t2.vip = 11, t1.realmoney, 0)) as `VIP11充值金额`
        ,CONCAT(FORMAT_NUMBER(CAST(sum(if(t2.vip = 11, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `VIP11订单占比`

        ,count(if(t2.vip = 12, t1.playerid, null)) as `VIP12充值人数`
        ,sum(if(t2.vip = 12, 1, 0)) as `VIP12充值订单数`
        ,sum(if(t2.vip = 12, t1.realmoney, 0)) as `VIP12充值金额`
        ,CONCAT(FORMAT_NUMBER(CAST(sum(if(t2.vip = 12, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `VIP12订单占比`

        ,count(if(t2.vip = 13, t1.playerid, null)) as `VIP13充值人数`
        ,sum(if(t2.vip = 13, 1, 0)) as `VIP13充值订单数`
        ,sum(if(t2.vip = 13, t1.realmoney, 0)) as `VIP13充值金额`
        ,CONCAT(FORMAT_NUMBER(CAST(sum(if(t2.vip = 13, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `VIP13订单占比`

        ,count(if(t2.vip = 14, t1.playerid, null)) as `VIP14充值人数`
        ,sum(if(t2.vip = 14, 1, 0)) as `VIP14充值订单数`
        ,sum(if(t2.vip = 14, t1.realmoney, 0)) as `VIP14充值金额`
        ,CONCAT(FORMAT_NUMBER(CAST(sum(if(t2.vip = 14, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `VIP14订单占比`

        ,count(if(t2.vip = 15, t1.playerid, null)) as `VIP15充值人数`
        ,sum(if(t2.vip = 15, 1, 0)) as `VIP15充值订单数`
        ,sum(if(t2.vip = 15, t1.realmoney, 0)) as `VIP15充值金额`
        ,CONCAT(FORMAT_NUMBER(CAST(sum(if(t2.vip = 15, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `VIP15订单占比`
    from
        {db_read}.ods_order_test as t1
    left join
        {db_read}.ods_log_order_test as t2
    on
        t1.platformorderid  = t2.platformorderid 
    LEFT JOIN
        {db_read}.ods_config_shop_test as t3
    on
        t1.goodsid = cast(t3.goods_id as string)
    WHERE
        t1.dt >= '{start_dt}' and t1.dt < '{end_dt}'
        AND t1.status = {status}
        and t2.step = {step}
        and t3.game_id = '1000'
    group by
        t1.dt, t1.goodsid, t3.goods_name
)

SELECT
    t1.`日期`
    ,t1.`商品id`
    ,t1.`商品名称`
    ,t1.`充值人数`
    ,t1.`充值订单数`
    ,t1.`充值金额`
    ,t1.`VIP0充值人数`
    ,t1.`VIP0充值订单数`
    ,t1.`VIP0充值金额`
    ,t1.`VIP0订单占比`
    ,t1.`VIP1充值人数`
    ,t1.`VIP1充值订单数`
    ,t1.`VIP1充值金额`
    ,t1.`VIP1订单占比`
    ,t1.`VIP2充值人数`
    ,t1.`VIP2充值订单数`
    ,t1.`VIP2充值金额`
    ,t1.`VIP2订单占比`
    ,t1.`VIP3充值人数`
    ,t1.`VIP3充值订单数`
    ,t1.`VIP3充值金额`
    ,t1.`VIP3订单占比`
    ,t1.`VIP4充值人数`
    ,t1.`VIP4充值订单数`
    ,t1.`VIP4充值金额`
    ,t1.`VIP4订单占比`
    ,t1.`VIP5充值人数`
    ,t1.`VIP5充值订单数`
    ,t1.`VIP5充值金额`
    ,t1.`VIP5订单占比`
    ,t1.`VIP6充值人数`
    ,t1.`VIP6充值订单数`
    ,t1.`VIP6充值金额`
    ,t1.`VIP6订单占比`
    ,t1.`VIP7充值人数`
    ,t1.`VIP7充值订单数`
    ,t1.`VIP7充值金额`
    ,t1.`VIP7订单占比`
    ,t1.`VIP8充值人数`
    ,t1.`VIP8充值订单数`
    ,t1.`VIP8充值金额`
    ,t1.`VIP8订单占比`
    ,t1.`VIP9充值人数`
    ,t1.`VIP9充值订单数`
    ,t1.`VIP9充值金额`
    ,t1.`VIP9订单占比`
    ,t1.`VIP10充值人数`
    ,t1.`VIP10充值订单数`
    ,t1.`VIP10充值金额`
    ,t1.`VIP10订单占比`
    ,t1.`VIP11充值人数`
    ,t1.`VIP11充值订单数`
    ,t1.`VIP11充值金额`
    ,t1.`VIP11订单占比`
    ,t1.`VIP12充值人数`
    ,t1.`VIP12充值订单数`
    ,t1.`VIP12充值金额`
    ,t1.`VIP12订单占比`
    ,t1.`VIP13充值人数`
    ,t1.`VIP13充值订单数`
    ,t1.`VIP13充值金额`
    ,t1.`VIP13订单占比`
    ,t1.`VIP14充值人数`
    ,t1.`VIP14充值订单数`
    ,t1.`VIP14充值金额`
    ,t1.`VIP14订单占比`
    ,t1.`VIP15充值人数`
    ,t1.`VIP15充值订单数`
    ,t1.`VIP15充值金额`
    ,t1.`VIP15订单占比`
FROM
    A t1
WHERE
    t1.`充值金额排名` <= 20
order by
    `日期` desc, `充值金额` desc