SELECT
    CONCAT(SUBSTRING(dt, 1, 4), '-', SUBSTRING(dt, 5, 2), '-', SUBSTRING(dt, 7, 2)) as `日期`
    ,recharge_times as `充值次数`

    ,sum(if(vip = 0, 1, 0)) as `vip0充值人数`
    ,CONCAT(FORMAT_NUMBER(CAST(sum(if(vip = 0, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `vip0充值人数比例`

    ,sum(if(vip = 1, 1, 0)) as `vip1充值人数`
    ,CONCAT(FORMAT_NUMBER(CAST(sum(if(vip = 1, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `vip1充值人数比例`

    ,sum(if(vip = 2, 1, 0)) as `vip2充值人数`
    ,CONCAT(FORMAT_NUMBER(CAST(sum(if(vip = 2, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `vip2充值人数比例`
    
    ,sum(if(vip = 3, 1, 0)) as `vip3充值人数`
    ,CONCAT(FORMAT_NUMBER(CAST(sum(if(vip = 3, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `vip3充值人数比例`
    
    ,sum(if(vip = 4, 1, 0)) as `vip4充值人数`
    ,CONCAT(FORMAT_NUMBER(CAST(sum(if(vip = 4, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `vip4充值人数比例`
    
    ,sum(if(vip = 5, 1, 0)) as `vip5充值人数`
    ,CONCAT(FORMAT_NUMBER(CAST(sum(if(vip = 5, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `vip5充值人数比例`
    
    ,sum(if(vip = 6, 1, 0)) as `vip6充值人数`
    ,CONCAT(FORMAT_NUMBER(CAST(sum(if(vip = 6, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `vip6充值人数比例`
    
    ,sum(if(vip = 7, 1, 0)) as `vip7充值人数`
    ,CONCAT(FORMAT_NUMBER(CAST(sum(if(vip = 7, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `vip7充值人数比例`
    
    ,sum(if(vip = 8, 1, 0)) as `vip8充值人数`
    ,CONCAT(FORMAT_NUMBER(CAST(sum(if(vip = 8, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `vip8充值人数比例`
    
    ,sum(if(vip = 9, 1, 0)) as `vip9充值人数`
    ,CONCAT(FORMAT_NUMBER(CAST(sum(if(vip = 9, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `vip9充值人数比例`
    
    ,sum(if(vip = 10, 1, 0)) as `vip10充值人数`
    ,CONCAT(FORMAT_NUMBER(CAST(sum(if(vip = 10, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `vip10充值人数比例`
    
    ,sum(if(vip = 11, 1, 0)) as `vip11充值人数`
    ,CONCAT(FORMAT_NUMBER(CAST(sum(if(vip = 11, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `vip11充值人数比例`
    
    ,sum(if(vip = 12, 1, 0)) as `vip12充值人数`
    ,CONCAT(FORMAT_NUMBER(CAST(sum(if(vip = 12, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `vip12充值人数比例`

    ,sum(if(vip = 13, 1, 0)) as `vip13充值人数`
    ,CONCAT(FORMAT_NUMBER(CAST(sum(if(vip = 13, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `vip13充值人数比例`
    
    ,sum(if(vip = 14, 1, 0)) as `vip14充值人数`
    ,CONCAT(FORMAT_NUMBER(CAST(sum(if(vip = 14, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `vip14充值人数比例`
    
    ,sum(if(vip = 15, 1, 0)) as `vip15充值人数`
    ,CONCAT(FORMAT_NUMBER(CAST(sum(if(vip = 15, 1, 0)) / count(*) AS DECIMAL(17, 15)) * 100, 1), '%') as `vip15充值人数比例`
FROM
(
    SELECT
        t1.dt
        ,t2.vip
        ,t1.playerid
        ,COUNT(*) as recharge_times
    from
        b1_statistics.ods_order_test as t1
    left join
        b1_statistics.ods_log_order_test as t2
    on
        t1.platformorderid  = t2.platformorderid 
    WHERE
        t1.dt >= '{start_dt}' and t1.dt < '{end_dt}' 
        AND t1.status = {status} 
        and t2.step = {step}
    GROUP BY
        t1.dt
        ,t2.vip
        ,t1.playerid
) as recharge_counts
GROUP BY
    dt, recharge_times
ORDER BY
    `日期` desc, `充值次数` asc