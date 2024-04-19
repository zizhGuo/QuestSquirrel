insert overwrite table guozizhun.dwd_3d_pacific_daily_lv1_users
PARTITION (dt = '{end_dt}') 
select 
distinct uid 
from b3_statistics.ods_log_common_obtain_reward 
where dt='{end_dt}' and configid = 0 and logtype = 'pacific_clean_reward'