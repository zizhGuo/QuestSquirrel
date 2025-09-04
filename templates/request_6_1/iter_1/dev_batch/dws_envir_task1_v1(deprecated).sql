With A as (
    select 
        t2.dt as stats_dt
        ,t1.uid
        ,max(t1.configid) as day_end_level_allhistory 
    from (select uid, configid, dt from b3_statistics.ods_log_common_obtain_reward where logType = 'pacific_clean_reward') t1
    inner join (
                select
                    dt
                from
                    guozizhun.dates_dimension
                where
                    dt between '{start_dt}' and '{end_dt}'
            ) t2 on t1.dt <= t2.dt
    group by t2.dt, t1.uid
)

,B as (
    select
        t1.stats_dt
        ,t1.uid
        ,t1.day_end_level_allhistory
        ,if(t2.uid is not null, 1, 0) as is_active_fairydemonland
        ,if(t3.player_id is not null, 1, 0) as is_welfare_users
    from
        A t1
        left join (
            select
                distinct dt,
                uid
            from
                b3_statistics.ods_log_gameonline t1
            where
                dt between '{start_dt}' and '{end_dt}'
                and gameid in (132, 133, 134)
                and playertype <> 4
        ) t2 on t1.stats_dt = t2.dt and t1.uid = cast(t2.uid as bigint)
        left join b3_statistics.ods_game_welfare_test t3
        on t1.uid = t3.player_id
)

,C1 as (
select t1.stats_dt, t1.day_end_level_allhistory, t2.level_name, count(t1.uid) as n_players_demonfairyland_level
from B t1
inner join guozizhun.config_fishery_pacific_3d t2
on t1.day_end_level_allhistory = t2.level and t2.type = 1
group by t1.stats_dt, t1.day_end_level_allhistory, t2.level_name
)

,D as (
select
    C1.stats_dt
    ,C1.day_end_level_allhistory
    ,C1.level_name
    ,C1.n_players_demonfairyland_level
    ,n_players_demonfairyland_level - lag(n_players_demonfairyland_level) over(partition by day_end_level_allhistory, level_name order by stats_dt) as n_players_diff
from
    C1
)

select
    stats_dt
    ,level_name
    ,n_players_demonfairyland_level
    ,n_players_diff
from
    D
order by stats_dt desc, day_end_level_allhistory