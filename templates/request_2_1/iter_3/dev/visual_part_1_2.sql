with A as (
select
  playerid
  ,vip
  ,push_price_type
  ,push_vip_type
  ,god_goods
  ,if(god_goods = 0, 1, 0) as regular_good
from
  guozizhun.game_god_grant_push_goods
where
  push_date = '{start_dt}'
  and playerid_order is not null
  and is_order_effective = 1
)
,

B as (
select
  playerid
  ,vip
  ,push_price_type
  ,push_vip_type
  ,case 
    when sum(god_goods) over(partition by playerid) > 0 and sum(regular_good) over(partition by playerid) > 0 then '都买了'
    when sum(god_goods) over(partition by playerid) > 0 and sum(regular_good) over(partition by playerid) = 0 then '只买了活动'
    else '只买了常规' end as purchase_type
from
  A
)

select
  purchase_type
  ,push_price_type
  ,push_vip_type
  ,vip
  ,count(distinct playerid) as n_players
from
  B
group by
  purchase_type
  ,push_price_type
  ,push_vip_type
  ,vip
order by
  purchase_type
  ,push_price_type
  ,push_vip_type
  ,vip