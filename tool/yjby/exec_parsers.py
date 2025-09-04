import os
import subprocess
import sys
import yaml
import time

CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_FILE_DIR)
GRANDPARENT_DIR = os.path.dirname(PARENT_DIR)
sys.path.insert(0, CURRENT_FILE_DIR)
sys.path.insert(1, PARENT_DIR)
sys.path.insert(2, GRANDPARENT_DIR)


from modules_fairyland.connector import HiveConnector
from modules_fairyland.query import QueryManager_manual
from main_fairydemon import Module

CONFIG = """
version: 'v5.10'
run_parser: 1
run_query: 1
connector:
  host: "172.19.121.24"
  port: "10000"
  hive_conf:
    "hive.resultset.use.unique.column.names": "false"
    "hive.execution.engine": "spark"
    "spark.executor.instances": "6"
    "spark.executor.cores": "8"
    "spark.driver.cores": "8"
  username: "hadoop"
  password: "hadoop"
  auth: "CUSTOM"

module:
  connector_module: 'connector'
  query_module: 'placeholder'
  report_module: 'placeholder'
  visual_module: 'placeholder'
  email_module: 'placeholder'

end_dt: 'placeholder'
start_dt: 'placeholder'

config_sql_path: "config_sql"
"""


def execute_python_files_in_dir(directory, args):
    """
    在指定目录下执行所有Python文件。
    
    :param directory: 目录的路径。
    """
    if not os.path.exists(directory):
        # create the directory
        os.makedirs(directory)

    # 遍历指定目录下的所有文件和目录
    for filename in os.listdir(directory):
        # 构建每个文件的完整路径
        filepath = os.path.join(directory, filename)
        
        # 检查当前文件是否是Python文件
        if os.path.isfile(filepath) and filename.endswith('.py'):
            print(f"Executing {filename}...")
            # 使用subprocess调用python执行文件
            subprocess.run(['python', filepath] + args, check=True)

def main():
    config = yaml.safe_load(CONFIG)
    print(config)

    if config.get('run_parser') and config['run_parser'] == 1:
        print('running parser scripts...')
        execute_python_files_in_dir(os.path.join(CURRENT_FILE_DIR, 'parser_scripts'), 
                                    args = ['--version', config['version'], '--customized', 'customized'])
        
    if config.get('run_query') and config['run_query'] == 1:
        print('running query scripts...')
        module = Module(config)
        connector = HiveConnector(config, CURRENT_FILE_DIR, module)
        queryner = QueryManager_manual()
        config_path = os.path.join(CURRENT_FILE_DIR, config['config_sql_path'], config['version'])
        print('config_path: ', config_path)
        sqls = queryner.load_all_query_from_dir(config_path)

        print('if sqls contains all.sql: ', 'all.sql' in sqls)
        print('sqls length: ', len(sqls))
        print('running sqls')
        task_start = time.time()
        for i, sql in enumerate(sqls):
            print('running sqls No: ', i)
            time_start = time.time()
            connector.just_query(sql)
            print('time cost: ', time.time() - time_start)
        print('time spend for task: ', time.time() - task_start)
        print('finished')

if __name__ == '__main__':
    main()