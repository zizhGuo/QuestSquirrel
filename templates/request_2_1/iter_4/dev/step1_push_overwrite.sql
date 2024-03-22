WITH A AS (
    SELECT
        t.playerid
        ,vip
        ,CASE
            WHEN vip in (1, 2, 3, 4, 5) THEN '1'
            WHEN vip in (6, 7, 8, 9, 10) THEN '6'
            WHEN vip in (11, 12, 13, 14, 15) THEN '30'
            else 'error' end as push_price_type
        ,CASE
            WHEN vip in (1, 2, 3, 4, 5) THEN 'V1-V5'
            WHEN vip in (6, 7, 8, 9, 10) THEN 'V6-V10'
            WHEN vip in (11, 12, 13, 14, 15) THEN 'V11-V15'
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
        WHERE dt BETWEEN '20240315' AND '20240323'
        UNION
        SELECT 
            playerid
            ,logouttime AS login_logout_time
        FROM b1_statistics.ods_record_login_test 
        WHERE dt BETWEEN '20240315' AND '20240323'
    ) tt ON t.playerid = tt.playerid 
    WHERE
        status = 2
        AND pushtype = 0
        AND to_date(createtime) = '2024-03-15'
        AND mon = '202403'
    GROUP BY
        t.playerid,
        vip,
        createtime,
        endtime
)

,B AS (
    SELECT
        A.*
        ,tt.uid AS playerid_order
        ,tt.logtime AS ordertime
        ,if(tt.logtime > A.createtime AND tt.logtime <= A.endtime, 1, 0) as is_order_effective
        ,tt.costcount
        ,tt.goods
    FROM
        A
    LEFT JOIN (
        SELECT
            uid
            ,logtime
            ,costcount
            ,goods
        FROM
            b1_statistics.ods_log_shop tt
        WHERE
            dt BETWEEN '20240315' AND '20240323'
            and costitemtype = 1015
    ) tt ON A.playerid = tt.uid
)

INSERT OVERWRITE TABLE guozizhun.game_god_grant_push_goods 
PARTITION (push_date = '20240315')
SELECT
    B.*,
    tt.goods_name,
    IF(tt.goods_id IN (9026, 9027, 9028, 9029, 9030, 9031, 9032, 9033), 1, 0) AS god_goods
FROM
    B
LEFT JOIN
    b1_statistics.ods_config_shop_test tt ON B.goods = tt.goods_id AND tt.game_id = '1000'