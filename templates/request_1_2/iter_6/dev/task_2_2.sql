SELECT
    CONCAT(SUBSTRING(t1.dt, 1, 4), '-', SUBSTRING(t1.dt, 5, 2), '-', SUBSTRING(t1.dt, 7, 2)) as createdate
    ,t1.realmoney
    ,t1.playerid
    ,t2.vip
from
    {db_read}.ods_order_test as t1
left join
    {db_read}.ods_log_order_test as t2
on
    t1.platformorderid  = t2.platformorderid 
WHERE
    t1.dt >= '{start_dt}' and t1.dt < '{end_dt}' AND t1.status = {status} and t2.step = {step}
