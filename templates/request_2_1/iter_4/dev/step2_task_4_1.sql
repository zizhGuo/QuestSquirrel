WITH A as (
SELECT
    playerid_order
    ,realmoney
    ,god_goods
    ,push_date
    ,DATEDIFF(date(ordertime), date(createtime)) AS days_from_pushed
FROM
    guozizhun.game_god_grant_push_goods
WHERE
    playerid_order is not null
)

SELECT
    push_date
    ,count(distinct if(god_goods = 1, playerid_order, null)) as god_goods_playerid_count
    ,count(distinct if(god_goods = 0 and days_from_pushed = 1, playerid_order, null)) as day1_playerid_count
    ,count(distinct if(god_goods = 0 and days_from_pushed = 1, playerid_order, null))/count(distinct if(god_goods = 1, playerid_order, null)) as day1_rate
    ,count(distinct if(god_goods = 0 and days_from_pushed = 2, playerid_order, null)) as day2_playerid_count
    ,count(distinct if(god_goods = 0 and days_from_pushed = 2, playerid_order, null))/count(distinct if(god_goods = 1, playerid_order, null)) as day2_rate
    ,count(distinct if(god_goods = 0 and days_from_pushed = 3, playerid_order, null)) as day3_playerid_count
    ,count(distinct if(god_goods = 0 and days_from_pushed = 3, playerid_order, null))/count(distinct if(god_goods = 1, playerid_order, null)) as day3_rate
    ,count(distinct if(god_goods = 0 and days_from_pushed = 4, playerid_order, null)) as day4_playerid_count
    ,count(distinct if(god_goods = 0 and days_from_pushed = 4, playerid_order, null))/count(distinct if(god_goods = 1, playerid_order, null)) as day4_rate
    ,count(distinct if(god_goods = 0 and days_from_pushed = 5, playerid_order, null)) as day5_playerid_count
    ,count(distinct if(god_goods = 0 and days_from_pushed = 5, playerid_order, null))/count(distinct if(god_goods = 1, playerid_order, null)) as day5_rate
    ,count(distinct if(god_goods = 0 and days_from_pushed = 6, playerid_order, null)) as day6_playerid_count
    ,count(distinct if(god_goods = 0 and days_from_pushed = 6, playerid_order, null))/count(distinct if(god_goods = 1, playerid_order, null)) as day6_rate
    ,count(distinct if(god_goods = 0 and days_from_pushed = 7, playerid_order, null)) as day7_playerid_count
    ,count(distinct if(god_goods = 0 and days_from_pushed = 7, playerid_order, null))/count(distinct if(god_goods = 1, playerid_order, null)) as day7_rate
FROM
    A
GROUP BY
    push_date
ORDER BY
    push_date desc