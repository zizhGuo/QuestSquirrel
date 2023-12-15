import os
import sys
current_file_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_file_dir)
sys.path.insert(0, current_file_dir)
sys.path.insert(1, parent_dir)

from modules.arguments import parser
from modules.config import ConfigManager
from modules.query import QueryManager
from modules.connector import HiveConnector
from modules.processor import DataProcessor
from modules.report import ReportGenerator
from modules.schedular import TaskScheduler
from modules.logger import Logger

CONFIG_FILE = 'config.yaml'

from datetime import datetime
now = datetime.now()


    


def main():
    print("entered main")
    # print(config['query']['params'])

    # test config reader
    # config_path = os.path.join(current_file_dir, CONFIG_FILE)
    # config_manager = ConfigManager(config_path)
    # config = config_manager.test_get_config()
    # print(config)

    config_path = os.path.join(current_file_dir, CONFIG_FILE)
    config_manager = ConfigManager(config_path)
    config = config_manager.test_get_config()
    # print(config)

    args = parser.parse_args()
    from datetime import timedelta, datetime
    from dateutil.relativedelta import relativedelta
    def date2str(date, timestamp_format=False):
        if timestamp_format:
            if date.day < 10:
                return f"{date.year}-{date.month}-0{date.day}"
            return f"{date.year}-{date.month}-{date.day}" 
        else:
            if date.day < 10:
                return f"{date.year}{date.month}0{date.day}"
            return f"{date.year}{date.month}{date.day}"
    end_dt = date2str(datetime.strptime(args.end_dt, "%Y-%m-%d"))
    end_date = date2str(datetime.strptime(args.end_dt, "%Y-%m-%d"), timestamp_format = True)
    start_dt = date2str(datetime.strptime(args.end_dt, "%Y-%m-%d") - timedelta(days=7))
    start_date = date2str(datetime.strptime(args.end_dt, "%Y-%m-%d") - timedelta(days=7), timestamp_format = True)
    pre_start_dt = date2str(datetime.strptime(args.end_dt, "%Y-%m-%d") - timedelta(days=7) - relativedelta(days=365))
    pre_start_date = date2str(datetime.strptime(args.end_dt, "%Y-%m-%d") - timedelta(days=7) - relativedelta(days=365), timestamp_format = True)
    # pre_start_dt = date2str(datetime.strptime(args.end_dt, "%Y-%m-%d") - timedelta(days=377))
    # print("end_dt = {}; start_dt = {}; pre_start_dt = {}".format(end_dt, start_dt, pre_start_dt))
    config['query']['params']['start_dt'] = start_dt
    config['query']['params']['start_date'] = start_date
    config['query']['params']['end_dt'] = end_dt
    config['query']['params']['end_date'] = end_date
    config['query']['params']['pre_start_dt'] = pre_start_dt
    config['query']['params']['pre_start_date'] = pre_start_date


    # test query manager
    # query = QueryManager(current_file_dir, config['query'])
    # query_runs = query.test_get_query()
    # for query_run in query_runs:
    #     print(query_run)
    #     print('------------------')

    # test connector
    query = "WITH v1 as ( SELECT t1.dt as createdate ,t2.vip ,MAX(t1.realmoney) as max_price ,MIN(t1.realmoney) as min_price from b1_statistics.ods_order_test as t1 left join b1_statistics.ods_log_order_test as t2 on t1.platformorderid = t2.platformorderid WHERE t1.dt >= '20231204' and t1.dt < '20231211' AND t1.status = 4 and t2.step = 99 GROUP BY t1.dt, t2.vip ), v2 as ( select dt as createdate ,1 as k ,count(distinct playerid) as total_players ,count(*) as total_records ,sum(realmoney) as total_amount from b1_statistics.ods_order_test WHERE dt >= '20231204' and dt < '20231211' AND status = 4 group by dt ), v3 as ( SELECT t1.createdate as createdate ,t1.vip ,COUNT(distinct t1.playerid) as vip_total_players ,COUNT(*) as vip_total_records ,SUM(t1.realmoney) as vip_total_amount ,COUNT(*) / SUM(COUNT(*)) over(partition by t1.createdate) as total_records_percentage ,MAX(t1.realmoney) as max_price ,COUNT(distinct if(t1.realmoney = t2.max_price, t1.playerid, NULL)) as max_price_players ,MIN(t1.realmoney) as min_price ,COUNT(distinct if(t1.realmoney = t2.min_price, t1.playerid, NULL)) as min_price_players ,1 as k FROM (select t1.dt as createdate ,t2.vip ,t1.playerid ,t1.realmoney from b1_statistics.ods_order_test as t1 left join b1_statistics.ods_log_order_test as t2 on t1.platformorderid = t2.platformorderid WHERE t1.dt >= '20231204' and t1.dt < '20231211' AND t1.status = 4 and t2.step = 99 ) as t1 LEFT JOIN v1 as t2 ON t1.createdate = t2.createdate and t1.vip = t2.vip GROUP BY t1.createdate, t1.vip ) select CONCAT(SUBSTRING(t1.createdate, 1, 4), '-', SUBSTRING(t1.createdate, 5, 2), '-', SUBSTRING(t1.createdate, 7, 2)) as `日期` ,if(t2.total_players is null, 0, t2.total_players) as `总充值人数` ,if(t2.total_records is null, 0, t2.total_records) as `总充值笔数` ,if(t2.total_amount is null, 0, t2.total_amount) as `总充值金额` ,t1.vip as `VIP等级` ,t1.vip_total_players as `VIP充值人数` ,t1.vip_total_records as `VIP充值笔数` ,t1.vip_total_amount as `VIP充值金额` ,CONCAT(FORMAT_NUMBER(CAST(t1.total_records_percentage AS DECIMAL(17, 15)) * 100, 1), '%') as `订单占比` ,t1.max_price as `充值最高档位` ,t1.max_price_players as `充值最高档位人数` ,t1.min_price as `充值最低档位` ,t1.min_price_players as `充值最低档位人数` from v3 as t1 LEFT JOIN v2 as t2 ON t1.createdate = t2.createdate and t1.k = t2.k order by `日期` desc, `VIP等级`"
    # query = "select * from guozizhun.game_god_grant_push_goods_case1_4 limit 10"
    connector = HiveConnector(config)
    print(query)
    connector.query_data(query)

    # connector.query_data(query_run)
    print("main end")
    pass

if __name__ == "__main__":
    main()


