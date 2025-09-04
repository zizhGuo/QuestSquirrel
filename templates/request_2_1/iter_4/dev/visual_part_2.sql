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
  ,count(distinct case when vip = 1 then playerid else null end) as n_vip_1_players
  ,count(distinct case when vip = 2 then playerid else null end) as n_vip_2_players
  ,count(distinct case when vip = 3 then playerid else null end) as n_vip_3_players
  ,count(distinct case when vip = 4 then playerid else null end) as n_vip_4_players
  ,count(distinct case when vip = 5 then playerid else null end) as n_vip_5_players
  ,count(distinct case when vip = 6 then playerid else null end) as n_vip_6_players
  ,count(distinct case when vip = 7 then playerid else null end) as n_vip_7_players
  ,count(distinct case when vip = 8 then playerid else null end) as n_vip_8_players
  ,count(distinct case when vip = 9 then playerid else null end) as n_vip_9_players
  ,count(distinct case when vip = 10 then playerid else null end) as n_vip_10_players
  ,count(distinct case when vip = 11 then playerid else null end) as n_vip_11_players
  ,count(distinct case when vip = 12 then playerid else null end) as n_vip_12_players
  ,count(distinct case when vip = 13 then playerid else null end) as n_vip_13_players
  ,count(distinct case when vip = 14 then playerid else null end) as n_vip_14_players
  ,count(distinct case when vip = 15 then playerid else null end) as n_vip_15_players
from
  B
group by
  purchase_type
order by
  purchase_type