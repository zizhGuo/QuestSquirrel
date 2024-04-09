With G as (select gameid from guozizhun.config_fishery_3d where fisheryname = 'pacific')
,fisherykill_prep as (
    select uid, dt, costgold, bonusid, bonuscount
    from b3_statistics.ods_log_fisherykill
    where dt >= '{start_dt}' and dt <= '{end_dt}'
    and gameid in (select gameid from guozizhun.config_fishery_3d where fisheryname = 'pacific')
)
,A as (
    select dt, sum(costgold) as sum_consumed_gold
    from fisherykill_prep
    group by dt
)

,B as (
    select t1.dt, sum(t1.bonuscount * t2.gold) as sum_acquired_gold
    from fisherykill_prep t1
    inner join guozizhun.config_item_gold_3d t2
    on t1.bonusid = t2.itemid
    group by t1.dt
)
,B1 as (
    select dt, sum(itemcount * gold) as sum_buff_gold
    from
    (   
        select
            dt, itemid, itemcount
        from
            (select dt, bonusitemid, bonuscount 
            from b3_statistics.ods_log_buff_extrareward t
            left join G
            on t.gameid = G.gameid
            where dt >= '{start_dt}' and dt <= '{end_dt}'
            and playertype != 4
            and G.gameid is not null) t
        lateral view posexplode(bonusitemid) t1 as idx, itemid
        lateral view posexplode(bonuscount) t2 as idx, itemcount
        where t1.idx = t2.idx
    ) t1
    join guozizhun.config_item_gold_3d t2
    on t1.itemid = t2.itemid
    group by dt
)
,B2 as (
    select dt, sum(itemcount * gold) as sum_common_special_gold
    from
    (   
        select
            dt, itemid, itemcount
        from
            (select dt, rewarditemid, rrewardcount
            from b3_statistics.ods_log_common_special_fish t
            left join G
            on t.gameid = G.gameid
            where dt >= '{start_dt}' and dt <= '{end_dt}'
            and G.gameid is not null
            and playertype != 4) t
        lateral view posexplode(rewarditemid) t1 as idx, itemid
        lateral view posexplode(rrewardcount) t2 as idx, itemcount
        where t1.idx = t2.idx 
    ) t1
    join guozizhun.config_item_gold_3d t2
    on t1.itemid = t2.itemid
    group by dt
)
,B3 as (
    select dt, sum(itemcount * gold) as sum_space_special_gold
    from
    (   
        select
            dt, itemid, itemcount
        from
            (select dt, rewarditemid, rewardcount
            from b3_statistics.ods_log_space_special_kill t
            left join G
            on t.gameid = G.gameid
            where dt >= '{start_dt}' and dt <= '{end_dt}'
            and G.gameid is not null
            and playertype != 4) t
        lateral view posexplode(rewarditemid) t1 as idx, itemid
        lateral view posexplode(rewardcount) t2 as idx, itemcount
        where t1.idx = t2.idx 
    ) t1
    join guozizhun.config_item_gold_3d t2
    on t1.itemid = t2.itemid
    group by dt
)
,B4 as (
    select dt, sum(itemcount * gold) as sum_space_battleship_gold
    from
    (   
        select
            dt, itemid, itemcount
        from
            (select dt, rewarditemid, rewardcount
            from b3_statistics.ods_log_space_battleship_kill t
            left join G
            on t.gameid = G.gameid
            where dt >= '{start_dt}' and dt <= '{end_dt}'
            and G.gameid is not null) t
        lateral view posexplode(rewarditemid) t1 as idx, itemid
        lateral view posexplode(rewardcount) t2 as idx, itemcount
        where t1.idx = t2.idx 
    ) t1
    join guozizhun.config_item_gold_3d t2
    on t1.itemid = t2.itemid
    group by dt
)
,B5 as (
    select dt, sum(itemcount * gold) as sum_drillskill_gold
    from
    (   
        select
            dt, itemid, itemcount
        from
            (select dt, rewarditemid, rewardcount
            from b3_statistics.ods_log_drillskill t
            left join G
            on t.fishery = G.gameid
            where dt >= '{start_dt}' and dt <= '{end_dt}'
            and G.gameid is not null
            and playertype != 4) t
        lateral view posexplode(rewarditemid) t1 as idx, itemid
        lateral view posexplode(rewardcount) t2 as idx, itemcount
        where t1.idx = t2.idx 
    ) t1
    join guozizhun.config_item_gold_3d t2
    on t1.itemid = t2.itemid
    group by dt
)
,B6 as (
    select t1.dt, sum(t2.valuemax * t3.gold) as sum_red_envolope_gold
    from b3_statistics.ods_log_red_envelope t1
    inner join guozizhun.config_fish_special_red_jackpot_3d t2
    on t1.redid = t2.id
    inner join guozizhun.config_item_gold_3d t3
    on t2.itemid = t3.itemid
    left join G
    on t1.gameid = G.gameid
    where t1.dt >= '{start_dt}' and t1.dt <= '{end_dt}' and t1.playertype != 4
    and G.gameid is not null
    group by t1.dt
)
,C as (
    select dt, sum(sum_acquired_gold) AS sum_gain_gold
    from
    (
        select dt, sum_acquired_gold from B
        union all
        select dt, sum_common_special_gold from B2
        union all
        select dt, sum_space_special_gold from B3
        union all
        select dt, sum_space_battleship_gold from B4
        union all
        select dt, sum_drillskill_gold from B5
        union all
        select dt, sum_red_envolope_gold from B6
    ) t
    group by dt
)
,C1 as (
    select dt, sum_buff_gold from B1
)
,D as (
    select
        dt, count(distinct uid) as n_fishery_uids, sum(onlinemilliseconds) / 60000 / count(distinct uid) as avg_online_minutes
    from b3_statistics.ods_log_gameonline t
    left join G
    on t.gameid = G.gameid
    where dt >= '{start_dt}' and dt <= '{end_dt}'
    and G.gameid is not null
    group by dt
)
,F1 as (
    select
    D.dt
    ,D.n_fishery_uids
    ,D.avg_online_minutes
    
    ,A.sum_consumed_gold
    ,C.sum_gain_gold
    ,if(C1.sum_buff_gold is not null, C1.sum_buff_gold, 0) as sum_buff_gold
    ,C.sum_gain_gold - A.sum_consumed_gold as sum_net_gold
    ,lag(C.sum_gain_gold - A.sum_consumed_gold) over(order by D.dt) as last_sum_net_gold

    from D
    inner join A
    on D.dt = A.dt
    inner join C
    on D.dt = C.dt
    left join C1
    on D.dt = C1.dt
)

,F2 as (
select
    dt
    ,n_fishery_uids
    ,n_fishery_uids - lag(n_fishery_uids) over(order by dt) as n_fishery_uids_diff
    ,avg_online_minutes
    ,avg_online_minutes - lag(avg_online_minutes) over(order by dt) as avg_online_minutes_diff
    ,sum_consumed_gold
    ,sum_consumed_gold - lag(sum_consumed_gold) over(order by dt) as sum_consumed_gold_diff
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
    dt,  
    format_number(n_fishery_uids, 0) as n_fishery_uids, 
    format_number(COALESCE(n_fishery_uids_diff, 0), 0) as n_fishery_uids_diff, 
    FORMAT_NUMBER(CAST(avg_online_minutes AS DECIMAL(20, 15)), 2) as avg_online_minutes,  
    FORMAT_NUMBER(CAST(COALESCE(avg_online_minutes_diff, 0) AS DECIMAL(20, 15)), 2) as avg_online_minutes_diff, 
    format_number(sum_consumed_gold, 0) as sum_consumed_gold,
    format_number(COALESCE(sum_consumed_gold_diff, 0), 0) as sum_consumed_gold_diff, 
    format_number(sum_net_gold, 0) as sum_net_gold, 
    format_number(COALESCE(sum_net_gold_diff, 0), 0) as sum_net_gold_diff
FROM F2
where
  dt = '{end_dt}'
order by dt desc