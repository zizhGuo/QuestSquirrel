import os
import sys
CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_FILE_DIR)
sys.path.insert(0, CURRENT_FILE_DIR)
sys.path.insert(1, PARENT_DIR)

from modules.query import QueryManager_manual
from modules_fairyland.connector import HiveConnector
from modules.config import ConfigManager

import pandas as pd

from datetime import datetime
now = datetime.now()

CONFIG_FILE = 'config_performance_test.yaml'



def main():
    config_path = os.path.join(CURRENT_FILE_DIR, CONFIG_FILE)
    config_manager = ConfigManager(config_path)
    config = config_manager.config
    print(config)

    sql1 = \
    """
        select * from guozizhun.dws_client_broke_exchange_task3_tmp
    """


    qm = QueryManager_manual()
    # sqls = {
    #     'optimized cp': qm.get_sql_run(sql1),
    #     # 'sql3': qm.get_sql_run(sql3)
    # }
    # sqls = {
    #     'sql3_distributednby': qm.get_sql_run(sql3)
    #     ,'sql4_groupbyby': qm.get_sql_run(sql4)
    # }
    print('sql:')
    print(sql1)
    connector = HiveConnector(config['connector'], CURRENT_FILE_DIR)
    df = connector.query_data(query = qm.get_sql_run(sql1), save_to_file = False, save_file_name = 'task_3_20240201_tmp.csv', fetch_result = 1)
    file = os.path.join(connector.root_path, connector.temp_save_path)+'/'+'task_3_20240201_tmp.csv'
    df.to_csv(file, index=False)
    print('finished')
if __name__ == "__main__":
    main()

