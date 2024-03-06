import os
from pyhive import hive
import pandas as pd
import time

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
        try:
            self.conn = hive.Connection(host=self.host, 
                                   port= self.port,
                                   configuration=self.hive_conf, 
                                   auth=self.auth,
                                   username=self.username,
                                   password=self.password
                                   )
            print(self.conn)
            # conn = hive.Connection(host=host, port=port, username=username, password=password, database=database)
        except Exception as e:
            print('Connection failed.')
            print(e)
            return

    def query_and_save(self, queries, i, tables):
        print('enter query_and_save')
        query = queries[i]
        table_name = tables[i]
        if query == 'na':
            return
        try:
            # start time
            time_start = time.now()

            cursor = self.conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            columns=[desc[0] for desc in cursor.description]
            # print(columns)
            print(f'Query No: {i}')
            # print current time
            time_end = time.now()
            print('results: {}'.format(results))
            print(f'Time Spent: {time_end - time_start}')

            df = pd.DataFrame(results, columns=columns)
            self.save_data(df, table_name)
        except Exception as e:
            if e == 'No result set to fetch from.':
                print('No result set to fetch from.')
                return
            print('Create dataframe failed.')
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
