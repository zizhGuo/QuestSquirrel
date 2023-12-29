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
        WHERE dt BETWEEN '{start_dt}' AND '{start_dt_plus_8}'
        UNION
        SELECT 
            playerid
            ,logouttime AS login_logout_time
        FROM b1_statistics.ods_record_login_test 
        WHERE dt BETWEEN '{start_dt}' AND '{start_dt_plus_8}'
    ) tt ON t.playerid = tt.playerid 
    WHERE
        status = 2
        AND pushtype = 0
        AND date(createtime) = '{start_date}'
        AND mon = '{dt_mon}'
    GROUP BY
        t.playerid,
        vip,
        createtime,
        endtime
),

B AS (
    SELECT
        A.*
        ,tt.playerid AS playerid_order
        ,tt.createtime AS ordertime
        ,if(tt.createtime > A.createtime AND tt.createtime <= A.endtime, 1, 0) as is_order_effective
        ,tt.realmoney
        ,tt.goodsid
    FROM
        A
    LEFT JOIN (
        SELECT
            playerid
            ,createtime
            ,realmoney
            ,goodsid
        FROM
            b1_statistics.ods_order_test tt
        WHERE
            dt BETWEEN '{start_dt}' AND '{start_dt_plus_8}'
            and status = 4
    ) tt ON A.playerid = tt.playerid
)
INSERT OVERWRITE TABLE guozizhun.game_god_grant_push_goods 
PARTITION (push_date = '{start_dt}')
SELECT
    B.*,
    tt.goods_name,
    IF(tt.goods_id IN (9026, 9027, 9028, 9029, 9030, 9031, 9032, 9033), 1, 0) AS god_goods
FROM
    B
LEFT JOIN
    b1_statistics.ods_config_shop_test tt ON B.goodsid = CAST(tt.goods_id AS STRING) AND tt.game_id = '1000'