With fisherykill_prep as (
    select uid, dt, gameid, costgold, bonusid, bonuscount
    from b3_statistics.ods_log_fisherykill
    where dt between '{start_dt}' and '{end_dt}'
)
,A as (
    select dt, gameid, sum(costgold) as sum_consumed_gold
    from fisherykill_prep
    group by dt, gameid
)

,B as (
    select t1.dt, t1.gameid, sum(t1.bonuscount * t2.gold) as sum_acquired_gold
    from fisherykill_prep t1
    inner join guozizhun.config_item_gold_3d t2
    on t1.bonusid = t2.itemid
    group by t1.dt, t1.gameid
)
,B1 as (
    select dt, gameid, sum(itemcount * gold) as sum_buff_gold
    from
    (   
        select
            dt, gameid, itemid, itemcount
        from
            b3_statistics.ods_log_buff_extrareward
        lateral view posexplode(bonusitemid) t1 as idx, itemid
        lateral view posexplode(bonuscount) t2 as idx, itemcount
        where dt between '{start_dt}' and '{end_dt}' and t1.idx = t2.idx 
        and playertype != 4
    ) t1
    join guozizhun.config_item_gold_3d t2
    on t1.itemid = t2.itemid
    group by dt, gameid
)
,B2 as (
    select dt, gameid, sum(itemcount * gold) as sum_common_special_gold
    from
    (   
        select
            dt, gameid, itemid, itemcount
        from
            b3_statistics.ods_log_common_special_fish
        lateral view posexplode(rewarditemid) t1 as idx, itemid
        lateral view posexplode(rrewardcount) t2 as idx, itemcount
        where dt between '{start_dt}' and '{end_dt}' and t1.idx = t2.idx 
        and playertype != 4
    ) t1
    join guozizhun.config_item_gold_3d t2
    on t1.itemid = t2.itemid
    group by dt, gameid
)
,B3 as (
    select dt, gameid, sum(itemcount * gold) as sum_space_special_gold
    from
    (   
        select
            dt, gameid, itemid, itemcount
        from
            b3_statistics.ods_log_space_special_kill
        lateral view posexplode(rewarditemid) t1 as idx, itemid
        lateral view posexplode(rewardcount) t2 as idx, itemcount
        where dt between '{start_dt}' and '{end_dt}' and t1.idx = t2.idx 
        and playertype != 4
    ) t1
    join guozizhun.config_item_gold_3d t2
    on t1.itemid = t2.itemid
    group by dt, gameid
)
,B4 as (
    select dt, gameid, sum(itemcount * gold) as sum_space_battleship_gold
    from
    (   
        select
            dt, gameid, itemid, itemcount
        from
            b3_statistics.ods_log_space_battleship_kill
        lateral view posexplode(rewarditemid) t1 as idx, itemid
        lateral view posexplode(rewardcount) t2 as idx, itemcount
        where dt between '{start_dt}' and '{end_dt}' and t1.idx = t2.idx 
    ) t1
    join guozizhun.config_item_gold_3d t2
    on t1.itemid = t2.itemid
    group by dt, gameid
)
,B5 as (
    select dt, gameid, sum(itemcount * gold) as sum_drillskill_gold
    from
    (   
        select
            dt, fishery as gameid, itemid, itemcount
        from
            b3_statistics.ods_log_drillskill
        lateral view posexplode(rewarditemid) t1 as idx, itemid
        lateral view posexplode(rewardcount) t2 as idx, itemcount
        where dt between '{start_dt}' and '{end_dt}' and t1.idx = t2.idx 
        and playertype != 4
    ) t1
    join guozizhun.config_item_gold_3d t2
    on t1.itemid = t2.itemid
    group by dt, gameid
)
,B6 as (
    select t1.dt, t1.gameid, sum(t2.valuemax * t3.gold) as sum_red_envolope_gold
    from b3_statistics.ods_log_red_envelope t1
    inner join guozizhun.config_fish_special_red_jackpot_3d t2
    on t1.redid = t2.id
    inner join guozizhun.config_item_gold_3d t3
    on t2.itemid = t3.itemid
    where t1.dt between '{start_dt}' and '{end_dt}' and t1.playertype != 4
    group by t1.dt, t1.gameid
)

,C as (
    select dt, gameid, sum(sum_acquired_gold) AS sum_gain_gold
    from
    (
        select dt, gameid, sum_acquired_gold from B
        union all
        select dt, gameid, sum_common_special_gold from B2
        union all
        select dt, gameid, sum_space_special_gold from B3
        union all
        select dt, gameid, sum_space_battleship_gold from B4
        union all
        select dt, gameid, sum_drillskill_gold from B5
        union all
        select dt, gameid, sum_red_envolope_gold from B6
    ) t
    group by dt, gameid
)
,C1 as (
    select dt, gameid, sum_buff_gold from B1
)
,D as (
    select
        dt, gameid, count(distinct uid) as n_fishery_uids, sum(onlinemilliseconds) / 60000 / count(distinct uid) as avg_online_minutes
    from b3_statistics.ods_log_gameonline 
    where dt between '{start_dt}' and '{end_dt}'
    group by dt, gameid
)
,F1 as (
    select
    D.dt
    ,D.gameid
    ,D.n_fishery_uids
    ,D.avg_online_minutes
    
    ,A.sum_consumed_gold
    ,A.sum_consumed_gold / sum(A.sum_consumed_gold) over(partition by D.dt) as consumed_gold_ratio
    ,C.sum_gain_gold
    ,if(C1.sum_buff_gold is not null, C1.sum_buff_gold, 0) as sum_buff_gold
    ,C.sum_gain_gold - A.sum_consumed_gold as sum_net_gold
    ,lag(C.sum_gain_gold - A.sum_consumed_gold) over(partition by D.gameid order by D.dt) as last_sum_net_gold

    from D
    inner join A
    on D.dt = A.dt and D.gameid = A.gameid
    inner join C
    on D.dt = C.dt and D.gameid = C.gameid
    left join C1
    on D.dt = C1.dt and D.gameid = C1.gameid
)

,F2 as (
select
    dt
    ,gameid
    ,n_fishery_uids
    ,n_fishery_uids - lag(n_fishery_uids) over(partition by gameid order by dt) as n_fishery_uids_diff
    ,avg_online_minutes
    ,avg_online_minutes - lag(avg_online_minutes) over(partition by gameid order by dt) as avg_online_minutes_diff
    ,sum_consumed_gold
    ,consumed_gold_ratio
    ,sum_consumed_gold - lag(sum_consumed_gold) over(partition by gameid order by dt) as sum_consumed_gold_diff
    ,sum_net_gold
    ,case 
        when sum_net_gold < 0 and last_sum_net_gold > 0 then -sum_net_gold+last_sum_net_gold
        when sum_net_gold > 0 and last_sum_net_gold < 0 then -sum_net_gold+last_sum_net_gold
        when sum_net_gold < 0 and last_sum_net_gold < 0 then last_sum_net_gold-sum_net_gold
        when sum_net_gold > 0 and last_sum_net_gold > 0 then last_sum_net_gold-sum_net_gold
        else 0 end as sum_net_gold_diff
from
 F1
)

SELECT
    t1.dt, 
    t1.gameid, 
    t2.fishery_name_new, 
    format_number(t1.n_fishery_uids, 0) as n_fishery_uids, 
    format_number(COALESCE(t1.n_fishery_uids_diff, 0), 0) as n_fishery_uids_diff, 
    FORMAT_NUMBER(CAST(t1.avg_online_minutes AS DECIMAL(20, 15)), 2) as avg_online_minutes, 
    FORMAT_NUMBER(CAST(COALESCE(t1.avg_online_minutes_diff, 0) AS DECIMAL(20, 15)), 2) as avg_online_minutes_diff, 
    format_number(t1.sum_consumed_gold, 0) as sum_consumed_gold, 
    CONCAT(FORMAT_NUMBER(CAST(t1.consumed_gold_ratio AS DECIMAL(20, 15))*100, 2), '%') as consumed_gold_ratio,
    format_number(COALESCE(t1.sum_consumed_gold_diff, 0), 0) as sum_consumed_gold_diff, 
    format_number(t1.sum_net_gold, 0) as sum_net_gold, 
    format_number(COALESCE(t1.sum_net_gold_diff, 0), 0) as sum_net_gold_diff
FROM F2 t1
left join guozizhun.config_fishery_3d t2
on t1.gameid = t2.gameid
where
  t1.gameid in {fishery_ids}
order by t1.dt desc, t1.gameid