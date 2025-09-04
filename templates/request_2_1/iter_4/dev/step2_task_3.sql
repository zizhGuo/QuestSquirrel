SELECT
    push_date
    ,push_vip_type
    ,push_price_type
    ,count(distinct playerid) as push_playerid_count
    ,count(
        distinct case
            WHEN total_login_count > 0 THEN playerid
            ELSE NULL
        END
    ) as logined_playerid_count
    ,count(
        distinct case
            WHEN god_goods = 1 AND is_order_effective = 1 THEN playerid
            ELSE NULL
        END
    ) as god_goods_playerid_count
    ,SUM(
        CASE
            WHEN god_goods = 1 AND is_order_effective = 1 THEN realmoney
            ELSE 0
        END
    ) as god_goods_realmoney
    ,count(
        distinct case
            WHEN god_goods = 1 AND is_order_effective = 1 THEN playerid
            ELSE NULL
        END
    )/count(
        distinct case
            WHEN total_login_count > 0 THEN playerid
            ELSE NULL
        END
    ) AS god_goods_rate
    ,count(
        distinct case
            WHEN is_order_effective = 1 THEN playerid_order
            ELSE NULL
            END
    ) as all_goods_playerid_count
    ,sum(
        case
            WHEN is_order_effective = 1 THEN realmoney
            ELSE 0
            END
        ) as all_goods_realmoney_sum
    ,count(distinct CASE WHEN is_order_effective = 1 THEN playerid_order ELSE NULL END) 
    / 
    count(distinct case WHEN total_login_count > 0 THEN playerid ELSE NULL END) as paid_rate
    ,SUM(CASE WHEN is_order_effective = 1 THEN realmoney ELSE 0 END) / count(distinct case WHEN total_login_count > 0 THEN playerid ELSE NULL END) as ARPU
    ,SUM(CASE WHEN is_order_effective = 1 THEN realmoney ELSE 0 END) / count(distinct CASE WHEN is_order_effective = 1 THEN playerid_order ELSE NULL END) as ARPPU
FROM
    guozizhun.game_god_grant_push_goods
where
    push_date in ('{last_start_dt}', '{start_dt}')
GROUP BY
    push_date
    ,push_vip_type
    ,push_price_type
order by
    cast(push_price_type as int)
    ,push_date desc
    ,push_vip_type