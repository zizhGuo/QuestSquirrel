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
    ,sum(if(god_goods = 0 and days_from_pushed = 1, realmoney, 0)) as day1_realmoney_sum
    ,count(if(god_goods = 0 and days_from_pushed = 1, playerid_order, null))/count(if(god_goods = 1, playerid_order, null)) as day1_sales_rate
    ,sum(if(god_goods = 0 and days_from_pushed = 2, realmoney, 0)) as day2_realmoney_sum
    ,count(if(god_goods = 0 and days_from_pushed = 2, playerid_order, null))/count(if(god_goods = 1, playerid_order, null)) as day2_sales_rate
    ,sum(if(god_goods = 0 and days_from_pushed = 3, realmoney, 0)) as day3_realmoney_sum
    ,count(if(god_goods = 0 and days_from_pushed = 3, playerid_order, null))/count(if(god_goods = 1, playerid_order, null)) as day3_sales_rate
    ,sum(if(god_goods = 0 and days_from_pushed = 4, realmoney, 0)) as day4_realmoney_sum
    ,count(if(god_goods = 0 and days_from_pushed = 4, playerid_order, null))/count(if(god_goods = 1, playerid_order, null)) as day4_sales_rate
    ,sum(if(god_goods = 0 and days_from_pushed = 5, realmoney, 0)) as day5_realmoney_sum
    ,count(if(god_goods = 0 and days_from_pushed = 5, playerid_order, null))/count(if(god_goods = 1, playerid_order, null)) as day5_sales_rate
    ,sum(if(god_goods = 0 and days_from_pushed = 6, realmoney, 0)) as day6_realmoney_sum
    ,count(if(god_goods = 0 and days_from_pushed = 6, playerid_order, null))/count(if(god_goods = 1, playerid_order, null)) as day6_sales_rate
    ,sum(if(god_goods = 0 and days_from_pushed = 7, realmoney, 0)) as day7_realmoney_sum
    ,count(if(god_goods = 0 and days_from_pushed = 7, playerid_order, null))/count(if(god_goods = 1, playerid_order, null)) as day7_sales_rate
FROM
    A
GROUP BY
    push_date
ORDER BY
    push_date desc