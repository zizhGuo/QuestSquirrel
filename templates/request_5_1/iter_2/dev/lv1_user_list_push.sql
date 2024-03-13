With A as (
select 
  uid
  ,dt
  ,cast(regexp_extract(ps, 'from:(\\d+)', 1) as int) AS lvfrom
  ,cast(regexp_extract(ps, 'lv:(\\d+)', 1) as int) AS lvto
from
  b1_statistics.ods_log_itemcirc
where
  dt = '{end_dt}'
  and circtype = 'fairy.demon.practice.lv.reward'
)

insert overwrite table guozizhun.dwd_2d_fairy_demon_land_daily_lv1_users_validation_2
PARTITION (dt = '{end_dt}')
select
  distinct uid
from
  A
where
  lvfrom = 1 and lvto = 1