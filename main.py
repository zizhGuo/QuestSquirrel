import os
import sys
CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_FILE_DIR)
sys.path.insert(0, CURRENT_FILE_DIR)
sys.path.insert(1, PARENT_DIR)

from modules.arguments import parser
from modules.config import ConfigManager
from modules.query import QueryManager
from modules.connector import HiveConnector
from modules.processor import DataProcessor
# from modules.processor import StrategyA
from modules.report import ReportGenerator
from modules.schedular import TaskScheduler
from modules.logger import Logger

import pandas as pd

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

    config_path = os.path.join(CURRENT_FILE_DIR, CONFIG_FILE)
    config_manager = ConfigManager(config_path)
    config = config_manager.config
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
    
    def date2str(date, timestamp_format=False):
        if date.month < 10:
            month = f"0{date.month}"
        else:
            month = f"{date.month}"
        if date.day < 10:
            day = f"0{date.day}"
        else:
            day = f"{date.day}"
        if timestamp_format:
            return f"{date.year}-{month}-{day}"
        else:
            return f"{date.year}{month}{day}"

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
    # print above config
    print(config['query']['params'])

    scheduler = TaskScheduler(config = config, root_path = CURRENT_FILE_DIR, date = end_dt)
    scheduler.run_task()
    scheduler.gen_report()
    scheduler.send_email()
    

    # connector.query_data(query_run)
    print("main end")
    pass

if __name__ == "__main__":
    main()


