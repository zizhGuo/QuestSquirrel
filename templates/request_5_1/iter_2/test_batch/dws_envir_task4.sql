With A as (
select 
  dt
  ,itemid
  ,`count`
  ,cast(regexp_extract(ps, 'gameId:(\\d+)', 1) as int) AS gameid
from b1_statistics.ods_log_itemcirc
where
    dt between '{start_dt}' and '{end_dt}'
    and ps like "%gameId%"
    and circtype in ( 'kill.treasurebox.fish.reward' , 
    'dragon.fishery.egg.reward' , 'ghost.copy.captain.reward' , 
    'kill.devour.box' , 'open.mermaid.treasure' , 'royal.box.reward' , 
    'ghost.poker.exchange.reward' , 'ghost.copy.captain.reward' , 
    'ghost.copy.fish.reward' , 'ghost.copy.ship.reward' , 'from.skeleton' , 
    'enviprot.pool.reward' , 'wingman.skill.reward' , 'wingman.fire.reward' , 
    'double.bullet.reward' , 'fairy.demon.skill.reward' )
    and itemid in (1001, 1016)
)

,A2 as (
select
    t1.dt
    ,cast(regexp_extract(t1.ps, 'gameId:(\\d+)', 1) as int) AS gameid
    ,t1.itemid
    ,t2.`desc`
    ,t1.`count`
    ,t2.gold
    ,t1.ps
from
    b1_statistics.ods_log_itemcirc t1
    inner join guozizhun.config_item_gold t2 on t1.itemid = t2.itemid
where
    t1.dt between '{start_dt}' and '{end_dt}'
    and t1.itemid not in (1001, 1016, 10001)
    and t1.ps like '%gameId%'
)

,A3 as (
select
    t1.dt
    ,t1.gameid
    ,t1.bonusid
    ,t2.`desc`
    ,t1.bonuscount
    ,t2.gold
from
    b1_statistics.ods_log_fisherykill_test t1
left join guozizhun.config_item_gold t2
on t1.bonusid = t2.itemid
where
    t1.dt between '{start_dt}' and '{end_dt}'
    and t1.gameid in (select id from guozizhun.config_fishery where fisheryname = 'fairy')
    and t1.bonusid not in (1001, 0, 5321)
)

,B as (
    select
        dt
        ,gameid
        ,sum(if(itemid = 1001, `count`, 0)) as sum_gold_1001
        ,sum(if(itemid = 1016, `count`, 0)) as sum_gold_1016
    from A
    where
        gameid in (select id from guozizhun.config_fishery where fisheryname = 'fairy')
    group by
        dt
        ,gameid
)

,C as (
    select
        dt
        ,gameid
        ,sum(bonuscount) as sum_bonuscount
    from
        b1_statistics.ods_log_fisherykill_test
    where
        dt between '{start_dt}' and '{end_dt}'
        and gameid in (select id from guozizhun.config_fishery where fisheryname = 'fairy')
        and bonusid = 1001
    group by
        dt
        ,gameid
)

,C1 as (
    select
        B.dt
        ,B.gameid
        ,B.sum_gold_1001 + C.sum_bonuscount - B.sum_gold_1016 as sum_gain_gold
    from B
    inner join C
    on B.dt = C.dt and B.gameid = C.gameid
)

,C2 as (
    select 
        dt
        ,gameid
        ,sum(abs(`count`) * gold) as sum_gold_item2gold
    from
        A2
    group by
        dt, gameid
)

,C3 as (
    select 
        dt
        ,gameid
        ,sum(abs(bonuscount) * gold) as sum_gold_bonus2gold
    from
        A3
    group by
        dt, gameid
)

,C4 as (
    select
        C2.dt
        ,C2.gameid
        ,C2.sum_gold_item2gold + C3.sum_gold_bonus2gold as sum_item_gold
    from C2
    inner join C3
    on C2.dt = C3.dt and C2.gameid = C3.gameid
)

,D as (
    select
        dt
        ,gameid
        ,count(distinct uid) as n_fishery_uids
        ,sum(onlinemilliseconds) / 60000 / count(distinct uid) as avg_online_minutes
    from b1_statistics.ods_log_gameonline 
    where dt between '{start_dt}' and '{end_dt}'
        and gameid in (select id from guozizhun.config_fishery where fisheryname = 'fairy') group by dt, gameid
)

,E as (
    select
        dt
        ,gameid
        ,sum(costgold) as sum_consumed_gold
    from
        b1_statistics.ods_log_fisherykill_test
    where
        dt between '{start_dt}' and '{end_dt}'
        and gameid in (select id from guozizhun.config_fishery where fisheryname = 'fairy')
    group by
        dt
        ,gameid
)

,F1 as (
    select
    D.dt
    ,D.gameid
    ,D.n_fishery_uids
    ,D.avg_online_minutes
    ,lag(D.avg_online_minutes) over(partition by D.gameid order by D.dt) as last_avg_online_minutes
    ,D.avg_online_minutes - lag(D.avg_online_minutes) over(partition by D.gameid order by D.dt) as avg_online_minutes_diff

    ,E.sum_consumed_gold
    ,E.sum_consumed_gold / sum(E.sum_consumed_gold) over(partition by D.dt) as consumed_gold_ratio

    ,C1.sum_gain_gold
    ,C1.sum_gain_gold / sum(C1.sum_gain_gold) over(partition by D.dt) as gain_gold_ratio

    ,C4.sum_item_gold
    ,C4.sum_item_gold / sum(C4.sum_item_gold) over(partition by D.dt) as item_gold_ratio

    ,C1.sum_gain_gold +  C4.sum_item_gold - E.sum_consumed_gold as sum_net_gold
    ,lag(C1.sum_gain_gold +  C4.sum_item_gold - E.sum_consumed_gold) over(partition by D.gameid order by D.dt) as last_sum_net_gold
    from D
    inner join E
    on D.dt = E.dt and D.gameid = E.gameid
    inner join C1
    on D.dt = C1.dt and D.gameid = C1.gameid
    inner join C4
    on D.dt = C4.dt and D.gameid = C4.gameid
)

,F as (
  select
    *, consumed_gold_ratio - lag(consumed_gold_ratio) over(partition by gameid order by dt) as consumed_gold_ratio_diff
  ,case 
        when sum_net_gold < 0 and last_sum_net_gold > 0 then -sum_net_gold+last_sum_net_gold
        when sum_net_gold > 0 and last_sum_net_gold < 0 then -sum_net_gold+last_sum_net_gold
        when sum_net_gold < 0 and last_sum_net_gold < 0 then last_sum_net_gold-sum_net_gold
        when sum_net_gold > 0 and last_sum_net_gold > 0 then last_sum_net_gold-sum_net_gold
        else 0 end as sum_net_gold_diff
  from F1
)

select
    dt
    ,t.name
    ,n_fishery_uids
    ,FORMAT_NUMBER(CAST(avg_online_minutes AS DECIMAL(17, 15)), 2) as avg_online_minutes
    ,FORMAT_NUMBER(CAST(avg_online_minutes_diff AS DECIMAL(17, 15)), 2) as avg_online_minutes_diff

    ,FORMAT_NUMBER(sum_consumed_gold, 0) as sum_consumed_gold
    ,CONCAT(FORMAT_NUMBER(CAST(consumed_gold_ratio AS DECIMAL(17, 15))*100, 2), '%') as consumed_gold_ratio
    ,CONCAT(FORMAT_NUMBER(CAST(consumed_gold_ratio_diff AS DECIMAL(17, 15))*100, 2), '%') as consumed_gold_ratio_diff

    ,FORMAT_NUMBER(sum_net_gold, 0) as sum_net_gold
    ,FORMAT_NUMBER(sum_net_gold_diff, 0) as sum_net_gold_diff
from F
left join guozizhun.config_fishery t
on F.gameid = t.id and t.type = 1
order by
    dt desc, t.id