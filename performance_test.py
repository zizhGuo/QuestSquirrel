import os
import sys
CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_FILE_DIR)
sys.path.insert(0, CURRENT_FILE_DIR)
sys.path.insert(1, PARENT_DIR)

from modules.query import QueryManager_manual
from modules.connector import HiveConnector
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
        select 
            t1.uid
            ,t1.bv
            ,t1.group_id
            ,t2.cur_push_id
            ,t2.cur_function_id
            ,t2.cur_func_name
        from guozizhun.dwd_client_broke_sequence t1
        left join guozizhun.dwd_client_broke_sequence_label t2
        on t1.uid = t2.uid and t1.dt = t2.dt and t1.group_id = t2.group_id and t1.layer = t2.layer and t2.dt between '20240119' and '20240119'
        where t1.is_welfare_user = 0 
        and t1.dt between '20240119' and '20240119'
    """

    sql2 = \
    """
        select 
            t1.*
            ,t2.cur_push_id
            ,t2.cur_function_id
            ,t2.cur_func_name
        from guozizhun.dwd_client_broke_sequence t1
        left join guozizhun.dwd_client_broke_sequence_label t2
        on t1.uid = t2.uid and t1.dt = t2.dt and t1.group_id = t2.group_id and t1.layer = t2.layer and t2.dt between '20240119' and '20240119'
        where t1.is_welfare_user = 0 
        and t1.dt between '20240119' and '20240119'
    """

    sql3 = \
    """
        select
        t.circtype
        ,count(1) as ct
        from
        (select circtype
        from
            b3_statistics.ods_log_itemcirc_test
        where
            dt between '20240116' and '20240117'
        DISTRIBUTE BY circtype) t
        group by
        t.circtype
    """ 

    sql4 = \
    """
        select
        circtype
        ,count(1) as ct
        from
        b3_statistics.ods_log_itemcirc_test
        where
        dt between '20240116' and '20240117'
        group by
        circtype
    """

    qm = QueryManager_manual()
    sqls = {
        'optimized cp': qm.get_sql_run(sql1),
        'regular push_down': qm.get_sql_run(sql2),
        # 'sql3': qm.get_sql_run(sql3)
    }
    sqls = {
        'sql3_distributednby': qm.get_sql_run(sql3)
        ,'sql4_groupbyby': qm.get_sql_run(sql4)
    }
    print('sqls:')
    print(sqls)
    print()
    connector = HiveConnector(config['connector'], CURRENT_FILE_DIR)
    
    # create dataframe empty
    df = pd.DataFrame()
    avg_time_taken = []
    # test running time
    import time
    import numpy as np
    for k, sql in sqls.items():
        _time_taken_list = []
        for j in range(1):
            start = time.time()
            connector.query_data(query = sql, save_to_file = False, save_file_name = 'temp.csv', fetch_result = 0)
            end = time.time()
            _time_taken_list.append(end - start)
        # append time_taken_list to df
        df[k] = _time_taken_list
        print('Time taken for {}'.format(k))    
        print(df[k])
        # print avg time taken
        print('Avg time taken: {}'.format(np.average(_time_taken_list)))
        avg_time_taken.append(np.average(_time_taken_list))
    # save df to CURRENT_FILE_DIR as csv
    df.to_csv(os.path.join(CURRENT_FILE_DIR, 'performance_test.csv'), index=False)
    print('rate: {}'.format((avg_time_taken[0] - avg_time_taken[1])/avg_time_taken[1]))

if __name__ == "__main__":
    main()

