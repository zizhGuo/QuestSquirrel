With A as (
select
    dt
    ,uid
    ,count
    ,costcount
    ,t2.goodsid
    ,t2.goodsname
    ,concat(t2.goodsname, "[", cast(t2.goodsid as string), "]") as goods
    ,t2.func_name
    ,t2.sub_func_name
    ,if(t3.player_id is not null, 1, 0) as is_from_welfare_user
from
    b1_statistics.ods_log_shop t1
inner join
    guozizhun.config_xianmo t2
on
    t1.goods = t2.goodsid
left join b1_statistics.ods_game_welfare_test t3
    on t1.uid = t3.player_id
where
    dt between '{start_dt}' and '{end_dt}'
    and costitemtype = 1015
)

,B as (
select
    dt
    ,goods
    ,costcount
    ,func_name
    ,sum(if(is_from_welfare_user = 0, 1, 0)) as n_non_welfare_exchange_orders
    ,sum(if(is_from_welfare_user = 0, costcount, 0)) as non_welfare_exchange_cost
    ,sum(if(is_from_welfare_user = 0, 1, 0)) / sum(sum(if(is_from_welfare_user = 0, 1, 0))) over(partition by dt) as non_welfare_exchange_orders_ratio
    ,sum(if(is_from_welfare_user = 0, costcount, 0)) / sum(sum(if(is_from_welfare_user = 0, costcount, 0))) over(partition by dt) as non_welfare_exchange_cost_ratio
from
    A
group by
    dt
    ,goods
    ,costcount
    ,func_name
)
,C as (
select
    dt
    ,func_name
    ,row_number() over(partition by dt, func_name order by n_non_welfare_exchange_orders desc) as rn
    ,goods
    ,costcount
    ,n_non_welfare_exchange_orders
    ,non_welfare_exchange_cost
    ,CONCAT(FORMAT_NUMBER(CAST(non_welfare_exchange_orders_ratio AS DECIMAL(17, 15))*100, 2), '%') as non_welfare_exchange_orders_ratio
    ,CONCAT(FORMAT_NUMBER(CAST(non_welfare_exchange_cost_ratio AS DECIMAL(17, 15))*100, 2), '%') as non_welfare_exchange_cost_ratio
    
    ,COALESCE(n_non_welfare_exchange_orders - lag(n_non_welfare_exchange_orders) over(partition by goods, func_name order by dt), 0) as n_non_welfare_exchange_orders_diff
    ,COALESCE(non_welfare_exchange_cost - lag(non_welfare_exchange_cost) over(partition by goods, func_name order by dt), 0) as non_welfare_exchange_cost_diff
    ,CONCAT(FORMAT_NUMBER(CAST(COALESCE(non_welfare_exchange_orders_ratio - lag(non_welfare_exchange_orders_ratio) over(partition by goods, func_name order by dt), 0) AS DECIMAL(17, 15))*100, 2), '%') as non_welfare_exchange_orders_ratio_diff
    ,CONCAT(FORMAT_NUMBER(CAST(COALESCE(non_welfare_exchange_cost_ratio - lag(non_welfare_exchange_cost_ratio) over(partition by goods, func_name order by dt), 0) AS DECIMAL(17, 15))*100, 2), '%') as non_welfare_exchange_cost_ratio_diff
from
    B
)
select
    *
from C
where dt = '{end_dt}' and rn <= 10
order by dt desc, func_name, rn