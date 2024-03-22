With break as (
    select 
        dt
        ,uid
        ,level-1 as level
        ,issuccess
    from 
        b3_statistics.ods_log_pacific_break_lair
    where
        dt between '{start_dt}' and '{end_dt}'
)
,B as (
    select 
        dt as stats_dt,
        uid,
        level
    from
        break
    where
        issuccess = 1
)
,A as (
    select 
        t1.dt as stats_dt,
        t1.uid,
        t1.level
    from
        break t1
    left join 
        B t2
    on t1.dt = t2.stats_dt and t1.uid = t2.uid and t1.level = t2.level
    where
        t1.issuccess = 0
        and t2.uid is null
)
,C1 as (
SELECT
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
            b3_statistics.ods_log_pacific_clean_level t1
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
having max(level) in (4, 9, 14)
order by
    stats_dt
)
,C2 as (
    select
        t1.stats_dt
        ,t1.uid
        ,t1.day_end_level_allhistory
        ,t1.day_end_level_allhistory + 1 as levelplus1
        ,if(t2.uid is not null, 1, 0) as is_active_fairydemonland
        ,if(t3.player_id is not null, 1, 0) as is_welfare_users
    from
        C1 t1
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
,C as (
    select
        t1.stats_dt,
        t1.uid,
        t1.day_end_level_allhistory
    from
        C2 t1
    left join
        break t2
    on t1.stats_dt = t2.dt and t1.uid = t2.uid and t1.day_end_level_allhistory = t2.level
    where t2.uid is null and t1.is_welfare_users = 0
)
,D2 as (
    select t1.stats_dt, t1.day_end_level_allhistory, t2.level_name, count(t1.uid) as n_players_demonfairyland_level
    from C2 t1
    inner join guozizhun.config_fishery_pacific_3d t2
    on t1.levelplus1 = t2.level and t2.type = 1
    where t1.is_welfare_users = 0
    group by t1.stats_dt, t1.day_end_level_allhistory, t2.level_name
)
,D as (
    select
        stats_dt
        ,level
        ,'仅失败' as success_type
        ,2 as rn
        ,cast(count(1) as string) as n_breakthroughs
        ,count(distinct uid) as n_players
    from
        A t1
    left join b3_statistics.ods_game_welfare_test t3
    on t1.uid = t3.player_id
    where t3.player_id is null
    group by
        stats_dt, level
    union
    select
        stats_dt
        ,level
        ,'成功'
        ,1
        ,cast(count(1) as string)
        ,count(distinct uid)
    from
        B t1
    left join b3_statistics.ods_game_welfare_test t3
    on t1.uid = t3.player_id
    where t3.player_id is null
    group by
        stats_dt, level
    union
    select
        stats_dt
        ,day_end_level_allhistory
        ,'未操作'
        ,3
        ,'/'
        ,count(distinct uid)
    from
        C
    group by
        stats_dt, day_end_level_allhistory
)
,D1 as (
select
    t1.stats_dt, t1.day_end_level_allhistory, t1.level_name, t1.n_players_demonfairyland_level + t2.n_players as n_players_demonfairyland_level
from
    D2 t1
inner join
    D t2
on
    t1.stats_dt = t2.stats_dt and t1.day_end_level_allhistory = t2.level and t2.success_type = '成功'

)

select
    t1.stats_dt
    ,t3.level_name
    ,n_players_demonfairyland_level as n_players_total
    ,t1.success_type
    ,t1.n_breakthroughs
    ,t1.n_players
from
    D t1
left join D1 t3
on t1.stats_dt = t3.stats_dt and t1.level = t3.day_end_level_allhistory
order by t1.stats_dt desc, t1.level, t1.rn