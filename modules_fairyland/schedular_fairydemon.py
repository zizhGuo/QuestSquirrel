from modules_fairyland.connector import HiveConnector
from modules_fairyland.query import QueryManager
from modules_fairyland.processor import DataProcessor
from modules_fairyland.visualizer import Visualizer
from modules_fairyland.report import ReportGenerator
from modules_fairyland.email import Email
from modules_fairyland.flag import Flag

"""_summary_
更新task schedule调度方式

1、运行querys save dataframes
    
"""
class TaskScheduler:
    def __init__(self, config, root_path, param_dt, module):
        self.config = config
        self.root_path = root_path
        self.param_dt = param_dt
        self.module = module

        self.config['end_dt'] = param_dt['end_dt']
        self.config['start_dt'] = param_dt['start_dt']

        assert self.config is not None, "Config is None"

        self.query_manager = QueryManager(self.config, self.root_path, self.param_dt, module)
        self.query_manager.print_query_runs()
        self.quries = self.query_manager.query_runs

        self.connector = HiveConnector(self.config, self.root_path, module)
        self.template_names = self.query_manager.template_names
        self.tasks_num = int(self.config[module.query_module]["tasks_num"])
        self.tables = self.config[module.query_module]["tables"]

    def run_querys(self):
        for i in range(self.tasks_num):
            try:
                print('running querys')
                print(self.quries[self.template_names[i]])
                self.connector.query_and_save(self.quries, self.template_names, i, self.tables)
                # print(df)
                print('running querys done')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
            except Exception as e:
                print(f'Query: {i} run querys failed.')
                print(e)

        
