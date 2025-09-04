WITH A AS (
    SELECT
        t.playerid
        ,vip
        ,CASE
            WHEN vip in {vip_range1} THEN '{vip_price1}'
            WHEN vip in {vip_range2} THEN '{vip_price2}'
            WHEN vip in {vip_range3} THEN '{vip_price3}'
            else 'error' end as push_price_type
        ,CASE
            WHEN vip in {vip_range1} THEN '{vip_tag1}'
            WHEN vip in {vip_range2} THEN '{vip_tag2}'
            WHEN vip in {vip_range3} THEN '{vip_tag3}'
            else 'error' end as push_vip_type
        ,createtime
        ,endtime
        ,SUM(IF(login_logout_time > createtime AND login_logout_time <= endtime, 1, 0)) AS total_login_count
    FROM
        b1_statistics.ods_game_god_grant_test t
    LEFT JOIN (
        SELECT 
            playerid
            ,logintime AS login_logout_time
        FROM b1_statistics.ods_record_login_test 
        WHERE dt BETWEEN '{start_dt}' AND '{end_dt}'
        UNION
        SELECT 
            playerid
            ,logouttime AS login_logout_time
        FROM b1_statistics.ods_record_login_test 
        WHERE dt BETWEEN '{start_dt}' AND '{end_dt}'
    ) tt ON t.playerid = tt.playerid 
    WHERE
        status = 2
        AND pushtype = 0
        AND to_date(createtime) = '{start_date}'
        AND mon = '{mon}'
    GROUP BY
        t.playerid,
        vip,
        createtime,
        endtime
)

,B AS (
    SELECT
        A.*
        ,if(t.player_id is not null, 1, 0) as is_from_welfare_user
        ,tt.uid AS playerid_order
        ,tt.logtime AS ordertime
        ,if(tt.logtime > A.createtime AND tt.logtime <= A.endtime, 1, 0) as is_order_effective
        ,tt.costcount
        ,tt.goods
    FROM
        A
    LEFT JOIN b1_statistics.ods_game_welfare_test t on A.playerid = t.player_id
    LEFT JOIN (
        SELECT
            uid
            ,logtime
            ,costcount
            ,goods
        FROM
            b1_statistics.ods_log_shop tt
        WHERE
            dt BETWEEN '{start_dt}' AND '{end_dt}'
            and costitemtype = 1015
    ) tt ON A.playerid = tt.uid
)

INSERT OVERWRITE TABLE guozizhun.game_god_grant_push_goods 
PARTITION (push_date = '{start_dt}')
SELECT
    B.playerid
    ,B.vip
    ,B.push_price_type
    ,B.push_vip_type
    ,B.createtime
    ,B.endtime
    ,B.total_login_count
    ,B.playerid_order
    ,B.ordertime
    ,B.is_order_effective
    ,B.costcount
    ,B.goods
    ,tt.goods_name
    ,IF(tt.goods_id IN (9026, 9027, 9028, 9029, 9030, 9031, 9032, 9033), 1, 0) AS god_goods
FROM
    B
LEFT JOIN
    b1_statistics.ods_config_shop_test tt ON B.goods = tt.goods_id AND tt.game_id = '1000'
where
  B.is_from_welfare_user = 0