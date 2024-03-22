with A as (
select
  distinct
  push_date
  ,playerid
  ,0 as buy
from
  guozizhun.game_god_grant_push_goods
where
  playerid_order is null 
  and total_login_count > 0
union
select
  distinct
  push_date
  ,playerid
  ,1 as buy
from
  guozizhun.game_god_grant_push_goods
where
  playerid_order is not null 
  and is_order_effective = 1 
  and total_login_count > 0
)
,

B as (
select
  t1.*
  ,t2.push_date_label
from 
  A as t1
inner join
      (
        select
          push_date
          ,row_number() over(order by push_date) as push_date_label
        from
          (
            SELECT
              distinct push_date
            FROM
              guozizhun.game_god_grant_push_goods
          ) T
      ) t2
on
  t1.push_date = t2.push_date
)
,
C as (
select
  *
  ,lag(push_date_label) over(partition by playerid order by push_date_label) as last_push_date_label
  ,lag(buy) over(partition by playerid order by push_date_label) as last_buy
from
  B t
)
,
D as (
select 
  *
  ,case 
    when push_date_label - last_push_date_label = 1 and buy = 1 and last_buy = 1 then 'repeat-buyer'
    when push_date_label - last_push_date_label = 1 and buy = 0 and last_buy = 0 then 'non-interest-buyer'
    when push_date_label - last_push_date_label = 1 and buy = 1 and last_buy = 0 then 'new-buyer'
    when push_date_label - last_push_date_label = 1 and buy = 0 and last_buy = 1 then 'lost-buyer'
    else 'other_cat' end as buyer_type
from C
)

select 
  push_date
  ,count(distinct case when buyer_type = 'repeat-buyer' then playerid else null end) as n_repeat_buyers
  ,count(distinct case when buyer_type = 'non-interest-buyer' then playerid else null end) as n_non_interest_buyers
  ,count(distinct case when buyer_type = 'new-buyer' then playerid else null end) as n_new_buyer
  ,count(distinct case when buyer_type = 'lost-buyer' then playerid else null end) as n_lost_buyer
from
  D
group by
  push_date
order by
  push_date desc