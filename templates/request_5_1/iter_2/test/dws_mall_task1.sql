With pre as (
select
  t1.dt
  ,t2.goodsid
  ,t2.goodsname
  ,t2.func_name
  ,t2.sub_func_name
from
  guozizhun.dates_dimension t1
join
  guozizhun.config_xianmo t2
where
 t1.dt between '{start_dt}' and '{end_dt}'

)

,A as (
select
    t2.dt
    ,uid
    ,count
    ,costcount
    ,t2.goodsid
    ,t2.goodsname
    ,t2.func_name
    ,t2.sub_func_name
    ,if(t3.player_id is not null, 1, 0) as is_from_welfare_user
from
    pre t2
left join
    (select goods, costitemtype, dt, uid, count, costcount from b1_statistics.ods_log_shop where dt between '{start_dt}' and '{end_dt}') t1
on
    t1.goods = t2.goodsid and t1.costitemtype = 1015 and t1.dt = t2.dt
left join b1_statistics.ods_game_welfare_test t3
    on t1.uid = t3.player_id
)

,B as (
select
    dt,
    func_name,
    sum(if(is_from_welfare_user = 0 and uid is not null, 1, 0)) as n_non_welfare_exchange_orders,
    sum(if(is_from_welfare_user = 0 and uid is not null, costcount, 0)) as non_welfare_exchange_cost,
    sum(if(is_from_welfare_user = 0 and uid is not null, 1, 0)) / sum(sum(if(is_from_welfare_user = 0 and uid is not null, 1, 0))) over(partition by dt) as non_welfare_exchange_orders_ratio,
    sum(if(is_from_welfare_user = 0 and uid is not null, costcount, 0)) / sum(sum(if(is_from_welfare_user = 0 and uid is not null, costcount, 0))) over(partition by dt) as non_welfare_exchange_cost_ratio
from
    A
group by
    dt,
    func_name
)
,C as (
select
    dt
    ,func_name
    ,n_non_welfare_exchange_orders
    ,non_welfare_exchange_cost
    ,CONCAT(FORMAT_NUMBER(CAST(non_welfare_exchange_orders_ratio AS DECIMAL(17, 15))*100, 2), '%') as non_welfare_exchange_orders_ratio
    ,CONCAT(FORMAT_NUMBER(CAST(non_welfare_exchange_cost_ratio AS DECIMAL(17, 15))*100, 2), '%') as non_welfare_exchange_cost_ratio

    ,n_non_welfare_exchange_orders - lag(n_non_welfare_exchange_orders) over(partition by func_name order by dt) as n_non_welfare_exchange_orders_diff
    ,non_welfare_exchange_cost - lag(non_welfare_exchange_cost) over(partition by func_name order by dt) as non_welfare_exchange_cost_diff
    ,CONCAT(FORMAT_NUMBER(CAST(non_welfare_exchange_orders_ratio - lag(non_welfare_exchange_orders_ratio) over(partition by func_name order by dt) AS DECIMAL(17, 15))*100, 2), '%') as non_welfare_exchange_orders_ratio_diff
    ,CONCAT(FORMAT_NUMBER(CAST(non_welfare_exchange_cost_ratio - lag(non_welfare_exchange_cost_ratio) over(partition by func_name order by dt) AS DECIMAL(17, 15))*100, 2), '%') as non_welfare_exchange_cost_ratio_diff
from
B
)
select * from C where dt = '{end_dt}'
order by dt desc, n_non_welfare_exchange_orders desc