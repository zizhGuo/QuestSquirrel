import os
from pyhive import hive
import pandas as pd
import time
from decorator import retry

class HiveConnector:
    def __init__(self, config, root_path):
        print('enter HiveConnector init.')
        self.config = config
        assert self.config is not None, "Config connector is None"
        # self.task_name = config['task_name']
        # self.pipeline_name = config['pipeline']
        self.root_path = root_path
        
        # connector
        self.host = config['connector']['host']
        self.port = config['connector']['port']
        self.hive_conf = config['connector']['hive_conf']
        self.username = config['connector']['username']
        self.password = config['connector']['password']
        self.auth = config['connector']['auth']
        self.temp_save_path = config['connector']['temp_save_path']
        # try:
        print('running function: {}'.format(self.create_connection.__name__))
        self.conn = self.create_connection()
        print('Connection successful. conn: {}'.format(self.conn))
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

    @retry(max_retries=3, retry_delay=10, exception_to_check=Exception)
    def query_results(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns=[desc[0] for desc in cursor.description]
        print('results: {}'.format(results))
        df = pd.DataFrame(results, columns=columns)
        if df.empty:
            print('Empty dataframe.')
            raise Exception('No result set to fetch from. from dataframe')
        return df

    def query_and_save(self, queries, i, tables):
        print('enter query_and_save')
        query = queries[i]
        table_name = tables[i]
        if query == 'na':
            return
        try:
            time_start = time.time()
            print(f'Query No: {i+1}')
            df = self.query_results(query)
            time_end = time.time()
            print(f'Time Spent: {time_end - time_start}')

            self.save_data(df, table_name)
        except Exception as e:
            if e == 'No result set to fetch from.':
                print('No result set to fetch from.')
                return
            print('query_and_save function failed.')
            print(e)

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
        
    
    def save_data(self, df, save_file_name):
        try:
            file = os.path.join(self.root_path, self.temp_save_path)+'/'+save_file_name
            df.to_csv(file, index=False)
            print('Save data successfully.')
        except Exception as e:
            print('Save data failed.')
            print(e)
