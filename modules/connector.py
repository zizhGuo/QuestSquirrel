import os
from pyhive import hive
import pandas as pd

class HiveConnector:
    def __init__(self, config, root_path):
        self.config = config
        assert self.config is not None, "Config connector is None"
        # self.task_name = config['task_name']
        # self.pipeline_name = config['pipeline']
        self.root_path = root_path
        
        # connector
        self.host = config['host']
        self.port = config['port']
        self.hive_conf = config['hive_conf']
        self.username = config['username']
        self.password = config['password']
        self.auth = config['auth']
        self.temp_save_path = config['temp_save_path']
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

    def query_data(self, query, save_to_file=False, save_file_name = 'temp.csv'):
        try:
            # 执行查询
            print('query: {}'.format(query))
            cursor = self.conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            columns=[desc[0] for desc in cursor.description]
            print(columns)
            df = pd.DataFrame(results, columns=columns)
            if save_to_file:
                self.save_data(df, save_file_name)
        except Exception as e:
            print('Create dataframe failed.')
            print(e)
        return df
        
    
    def save_data(self, df, save_file_name):
        try:
            file = os.path.join(self.root_path, self.temp_save_path)+'/'+save_file_name
            df.to_csv(file, index=False)
            print('Save data successfully.')
        except Exception as e:
            print('Save data failed.')
            print(e)
