select
  goods_name
  ,push_price_type
  ,push_vip_type
  ,vip
  ,count(distinct playerid_order) as n_players
from
  guozizhun.game_god_grant_push_goods
where
  push_date = '{start_dt}' and playerid_order is not null and is_order_effective = 1
GROUP BY
  goods_name
  ,push_price_type
  ,push_vip_type
  ,vip
order by
  n_players desc