With A as (
(SELECT
    stats_dt,
    uid,
    max(level) as day_end_level_allhistory
from
    (
        select
            t1.dt,
            t1.uid,
            t1.level,
            t2.dt as stats_dt
        from
            b1_statistics.ods_log_fairy_demon_land_level_up_test t1
            inner join (
                select
                    dt
                from
                    guozizhun.dates_dimension
                where
                    dt between '{start_dt}' and '{end_dt}'
            ) t2 on t1.dt <= t2.dt
    ) t
group by
    stats_dt,
    uid
)
union
(select
  distinct
    t2.stats_dt
    ,t1.uid
    ,1 as day_end_level_allhistory
from guozizhun.dwd_2d_fairy_demon_land_daily_lv1_users_validation_2 t1
inner join (
    select
        dt as stats_dt
    from
        guozizhun.dates_dimension
    where
        dt between '{start_dt}' and '{end_dt}'
) t2 on t1.dt <= t2.stats_dt
left join b1_statistics.ods_log_fairy_demon_land_level_up_test t3
on t1.uid = t3.uid and t3.dt <= t2.stats_dt
where t3.uid is null
)
)
,
B as (
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
                b1_statistics.ods_log_gameonline t1
            where
                dt between '{start_dt}' and '{end_dt}'
                and gameid in (select id from guozizhun.config_fishery where fisheryname = 'fairy')
        ) t2 on t1.stats_dt = t2.dt and t1.uid = t2.uid
        left join b1_statistics.ods_game_welfare_test t3
        on t1.uid = t3.player_id
)

,C1 as (
select t1.stats_dt, t1.day_end_level_allhistory, t2.level_name, count(t1.uid) as n_players_demonfairyland_level
from B t1
inner join guozizhun.config_fairy_land t2
on t1.day_end_level_allhistory = t2.level and t2.type = 1
where t1.is_welfare_users = 0
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
where
    stats_dt = '{end_dt}'
order by stats_dt desc, day_end_level_allhistory

