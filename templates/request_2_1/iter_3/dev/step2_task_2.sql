SELECT
    push_date
    ,push_vip_type
    ,push_price_type
    ,count(distinct if(vip = 1, playerid_order, null)) as v1_n_playerids
    ,count(distinct if(vip = 2, playerid_order, null)) as v2_n_playerids
    ,count(distinct if(vip = 3, playerid_order, null)) as v3_n_playerids
    ,count(distinct if(vip = 4, playerid_order, null)) as v4_n_playerids
    ,count(distinct if(vip = 5, playerid_order, null)) as v5_n_playerids
    ,count(distinct if(vip = 6, playerid_order, null)) as v6_n_playerids
    ,count(distinct if(vip = 7, playerid_order, null)) as v7_n_playerids
    ,count(distinct if(vip = 8, playerid_order, null)) as v8_n_playerids
    ,count(distinct if(vip = 9, playerid_order, null)) as v9_n_playerids
    ,count(distinct if(vip = 10, playerid_order, null)) as v10_n_playerids
    ,count(distinct if(vip = 11, playerid_order, null)) as v11_n_playerids
    ,count(distinct if(vip = 12, playerid_order, null)) as v12_n_playerids
    ,count(distinct if(vip = 13, playerid_order, null)) as v13_n_playerids
    ,count(distinct if(vip = 14, playerid_order, null)) as v14_n_playerids
    ,count(distinct if(vip = 15, playerid_order, null)) as v15_n_playerids
FROM
    guozizhun.game_god_grant_push_goods
WHERE
    push_date in ('{last_start_dt}', '{start_dt}')
    and playerid_order is not null
    and god_goods = 1
    and is_order_effective = 1
group by
    push_date
    ,push_vip_type
    ,push_price_type
order by
    push_vip_type
    ,push_price_type
    ,push_date