import sys
import os
print('append module path: {}'.format(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from pyhive import hive
import pandas as pd
import time
from decorators import retry
import logging
logger = logging.getLogger('connector_logger')
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

from contextlib import contextmanager

@contextmanager
def managed_connection(connection: hive.Connection):
    try:
        yield connection
    finally:
        connection.close()
        logger.debug('connection closed.')

@contextmanager
def managed_cursor(connection: hive.Connection):
    cursor = connection.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
        logger.debug('cursor closed.')

class HiveConnector:
    def __init__(self, config, root_path, module):
        if not config:
            return
        print('enter HiveConnector init.')
        self.module = module
        self.config = config
        assert self.config is not None, "Config connector is None"
        # self.task_name = config['task_name']
        # self.pipeline_name = config['pipeline']
        self.root_path = root_path
        self.end_dt = config['end_dt']
        self.start_dt = config['start_dt']
        
        # connector
        self.host = config[module.connector_module]['host']
        self.port = config[module.connector_module]['port']
        self.hive_conf = config[module.connector_module]['hive_conf']
        self.username = config[module.connector_module]['username']
        self.password = config[module.connector_module]['password']
        self.auth = config[module.connector_module]['auth']
        # self.temp_save_path = config[module.query_module][sub_task]['temp_save_path']
        # try:
        print('running function: {}'.format(self.create_connection.__name__))

        # TODO: 初始多线程，comment下面两行
        self.conn = self.create_connection()
        print('Connection successful. conn: {}'.format(self.conn))


        #     print(self.conn)
        #     # conn = hive.Connection(host=host, port=port, username=username, password=password, database=database)
        # except Exception as e:
        #     print('Connection failed.')
        #     print(e)
        #     return

    @retry(max_retries=3, retry_delay=5, exception_to_check=Exception)
    def create_connection(self):
        return hive.Connection(host=self.host, 
                                   port= self.port,
                                   configuration=self.hive_conf, 
                                   auth=self.auth,
                                   username=self.username,
                                   password=self.password
                                   )

    # @retry(max_retries=3, retry_delay=10, exception_to_check=Exception)
    # def query_results(self, query, connection):
    #     with managed_cursor(connection) as cursor:
    #         cursor.execute(query, async_=True)
    #         results = cursor.fetchall()
    #         columns=[desc[0] for desc in cursor.description]
    #         print('results: {}'.format(results))
    #         df = pd.DataFrame(results, columns=columns)
    #         if df.empty: # TODO 错误或者正常无结果都可能empty
    #             print('Empty dataframe.')
    #             raise Exception('No result set to fetch from. from dataframe')
    #         return df

    @retry(max_retries=3, retry_delay=10, exception_to_check=Exception)
    def query_results_log(self, query, connection):
        with managed_cursor(connection) as cursor:
            cursor.execute(query, async_=True)
            status = cursor.poll().operationState
            while status in (hive.ttypes.TOperationState.INITIALIZED_STATE, hive.ttypes.TOperationState.RUNNING_STATE):
                logs = cursor.fetch_logs()
                for message in logs:
                    print(message)
                # 小睡一会儿等待更多日志
                time.sleep(1)
                status = cursor.poll().operationState
            logs = cursor.fetch_logs()
            for message in logs:
                print(message)

            results = cursor.fetchall()
            columns=[desc[0] for desc in cursor.description]
            cursor.close()

            # print('results: {}'.format(results))
            df = pd.DataFrame(results, columns=columns)
            if df.empty:
                print('Empty dataframe.')
                raise Exception('No result set to fetch from. from dataframe')
            return df

    @retry(max_retries=3, retry_delay=10, exception_to_check=Exception)
    def query_results(self, query, connection):
        with managed_cursor(connection) as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            columns=[desc[0] for desc in cursor.description]
            cursor.close()

            # print('results: {}'.format(results))
            df = pd.DataFrame(results, columns=columns)
            if df.empty:
                print('Empty dataframe.')
                raise Exception('No result set to fetch from. from dataframe')
            return df

    @retry(max_retries=3, retry_delay=10, exception_to_check=Exception)
    def just_query(self, query, connection):
        with managed_cursor(connection) as cursor:
            cursor.execute(query)

    def query_and_save(self, queries, template_names, i, tables, flags, temp_save_path, DBUG_LOGS):
        print('enter query_and_save')
        query = queries[template_names[i]]
        table_name = tables[i]
        if query == 'na':
            return
        try:
            if tables[i] != 'na' and flags[i]:
                format = 'xlsx'
                if tables[i].split('.')[1] == 'csv':
                    format = 'csv'
                # start time
                time_start = time.time()
                # df = self.query_results(query)
                with managed_connection(self.conn) as connection:
                    if DBUG_LOGS == 1:
                        df = self.query_results_log(query, connection)
                    else:
                        df = self.query_results(query, connection)
                time_end = time.time()
                print('results in df: {}'.format(df))
                print(f'Time Spent: {time_end - time_start}s')
                self.save_data(df, table_name, temp_save_path, format)
            elif tables[i] != 'na' and flags[i] == 0:
                print('Skip this query.')
            elif tables[i] == 'na' and flags[i]:
                time_start = time.time()
                with managed_connection(self.conn) as connection:
                    df = self.just_query(query, connection)
                time_end = time.time()
                print('results in df: {}'.format(df))
                print(f'Time Spent: {time_end - time_start}s')
            elif tables[i] == 'na' and flags[i] == 0:
                print('Skip this query.')
            else:
                print('No table to save to.')
                
        except Exception as e:
            if e == 'No result set to fetch from.':
                print('No result set to fetch from.')
                return
            print('Create dataframe failed.')
            print(e)
            return 'failed'
        else:
            return 'successful'
        # finally:
            # check if self.conn is closed or not
            # if not self.conn.closed:
                # self.conn.close()
                # logger.debug('query_and_save: self.conn not close, so close finally close it')
        
    def query_result_save(self):
        print('enter query_and_save')
        try:
            logger.debug('Connector: query_and_save entered.')
            time.sleep(2)
            logger.debug('Connector: query_and_save sleep done.')
                
        except Exception as e:
            if e == 'No result set to fetch from.':
                print('No result set to fetch from.')
                return
            print('Create dataframe failed.')
            print(e)
        else:
            return 'successful'
            
    def close(self):
        """
            deprecated
        """
        if not self.conn.closed:
            self.conn.close()

    def query_data(self, query, save_to_file=False, save_file_name = 'temp.csv', fetch_result = 1):
        try:
            # 执行查询
            # print('query: {}'.format(query))
            cursor = self.conn.cursor()
            cursor.execute(query)
            if fetch_result:
                results = cursor.fetchall()
                columns=[desc[0] for desc in cursor.description]
                print(columns)
                df = pd.DataFrame(results, columns=columns)
            else:
                # df = None
                # create an empty dataframe
                df = pd.DataFrame(columns=['col1','col2'])
            if save_to_file:
                self.save_data(df, save_file_name)
            return df
        except Exception as e:
            print('Create dataframe failed.')
            print(e)
    
    def save_data(self, df, save_file_name, temp_save_path, format='xlsx'):
        try:
            dir_path = os.path.join(self.root_path, temp_save_path, self.end_dt)
            file_path = os.path.join(dir_path, save_file_name)
            # file = os.path.join(self.root_path, self.temp_save_path)+'/'+ self.end_dt + '/'+save_file_name
            os.makedirs(dir_path, exist_ok=True)
            if format == 'csv':
                df.to_csv(file_path, index=False)
            else:
                df.to_excel(file_path, index=False, engine='openpyxl')
            # df.to_csv(file, index=False)
            print('Save data successfully.')
        except Exception as e:
            print('Save data failed.')
            print(e)

from unittest import mock

def test_query_results_log():
    """
        mock测试query_results_log
    """
    mock_cursor = mock.MagicMock()
    mock_connection = mock.MagicMock()
    mock_connection.cursor.return_value = mock_cursor

    # 模拟cursor行为
    mock_cursor.poll.return_value.operationState = "FINISHED_STATE"
    mock_cursor.fetch_logs.return_value = ["Log line 1", "Log line 2"]
    mock_cursor.fetchall.return_value = [(1, "result1"), (2, "result2")]
    mock_cursor.description = [("col1",), ("col2",)]


    client = HiveConnector([], [], [])
    client.conn = mock_connection
    query = "SELECT * FROM your_table"

    # 创建模拟的cursor和connection对象
    try:
        df = client.query_results_log(query)
        print(df)
    except Exception as e:
        print(f"Exception: {e}")
    
    mock_cursor.close.assert_called_once()
    mock_connection.close.assert_called_once()

# test_query_results_log()