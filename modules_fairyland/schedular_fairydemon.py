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
    def __init__(self, config, root_path, param_dt):
        self.config = config
        self.root_path = root_path
        self.param_dt = param_dt
        self.config['end_dt'] = param_dt['end_dt']
        self.config['start_dt'] = param_dt['start_dt']

        assert self.config is not None, "Config is None"
        assert self.config['query'] is not None, "query is None"
        
        self.query_manager = QueryManager(self.config, self.root_path, self.param_dt)
        self.query_manager.print_query_runs()
        self.quries = self.query_manager.query_runs
        self.connector = HiveConnector(self.config, self.root_path)
        self.template_names = self.query_manager.template_names
        self.tasks_num = int(self.config['query']["tasks_num"])
        self.tables = self.config['query']["tables"]

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


    def _init_flag(self):
        try:
            self.flag = Flag(self.config['flag'])
        except Exception as e:
            print('flag init outside failed.')
            print(e)
            return

    def _init_query_manager(self):
        self.query_manager = QueryManager(self.config, self.root_path, self.param_dt)
    
    def _init_connector(self):
        self.connector = HiveConnector(self.config['connector'], self.root_path)

    def _init_report_generator(self):
        self.report_generator = ReportGenerator(self.config['report'], self.root_path, self.date)

    def _init_tasks(self):
        try:
            self.tasks = {}
            for k, v in self.config['task'].items():
                self.tasks[k] = v
            print(self.tasks)
        except Exception as e:
            print('TaskScheduler init failed.')
            print(e)
            return
    
    def _init_email(self):
        self.email = Email(self.config['email'], self.root_path, self.date)
        # print(self.email.to_string())


    def gen_report_test(self, write_xlsx):
        import pandas as pd
        df1 = pd.DataFrame({
        'A': [1, 2],
        'B': [3, 4]
        })

        df2 = pd.DataFrame({
            'X': [1, 2, 9],
            'Y': [3, 4, 12],
            'Z': [7, 7, 7]
        })  

        df3 = pd.DataFrame({
            'C': [5, 6],
            'D': [7, 8],
            'E': [9, 10]
        })
        df4 = pd.DataFrame({
            'i': [5, 6],
            'y': [7, 8],
            'j': [9, 10]
        })
        df5 = pd.DataFrame({
            'z': [0, 90],
            'x': [0, 12],
            't': [0, 45]
        })
        df_write = [df1, df2, df3, df4, df5]
        sheet_name = ['na', 'sheet1', 'sheet1', 'sheet2', 'sheet3']
        # tables = {
        # 'sheet1': [df1,df2]
        # ,'sheet2': [df3]
        # ,'sheet3': [df4]
        # ,'sheet4': [df2]
        # ,'sheet5': [df1]
        # }
        try:
          self.report_generator.generate_god_batch(df_write, sheet_name, self.mapper, write_xlsx)
        except Exception as e:
            print('gen report init failed.')
            print(e)
            return

    def gen_report(self, write_xlsx):
        try:
          self.report_generator.generate_god_batch(self.df_write, self.sheet_name, self.mapper, write_xlsx)
        except Exception as e:
            print('gen report init failed.')
            print(e)
            return
        # try:
        #   self.report_generator.generate_visual(self.visual_write)
        # except Exception as e:
        #     print('gen report init failed.')
        #     print(e)
        #     return
        
    def send_email(self):
        self.email.send_email()


    def run_task(self):
        # config = self.config_manager.get_task_config(task_name)
        # 根据配置运行任务
        for task_k, task in self.tasks.items():
            print("New task round starts.")
            print("print current task: {}".format(task))
            if task_k not in self.flag.running_tasks:
                print('task {} is not in config file, skip to next task'.format(task_k))
                continue
            try:
                df = None
                graphs = None
                if task.get('hive_query') and self.flag.hivequery:
                    print('enter hive_query')
                    _config = task.get('hive_query')
                    quries = self.query_manager.query_runs
                    assert _config['template_name'] in quries, "tempalte is not in query_runs"
                    df = self.connector.query_data(quries[_config['template_name']], 
                                                    save_to_file=_config['save_to_file'], 
                                                    save_file_name = _config['save_file_name'],
                                                    fetch_result = _config.get('fetch_result')
                                                    )
                    
                if task.get('data_process') and self.flag.dataprocess:
                    print('enter data_process')
                    _config = task.get('data_process')
                    _config['read_from_file'] = self.flag.read_from_file_dataprocess
                    processor = DataProcessor(_config, self.root_path)
                    df = processor.process(df)
                    
                if task.get('visual_generate') and self.flag.visual:
                    _config = task.get('visual_generate')
                    _config['read_from_file'] = self.flag.read_from_file_visual
                    visulizer = Visualizer(_config, self.root_path)
                    graphs = visulizer.gen(df)
                if task.get('report_generate') and self.flag.reportgenerate:
                    _config = task.get('report_generate')
                    pass
                if task.get('logging') and self.flag.logging:
                    _ = task.get('logging')
                    pass
                if task.get('email') and self.flag.email:
                    _ = task.get('email')
                    pass
                print('-----------------------------------')
            except Exception as e:
                print('TaskScheduler run task failed.')
                print(e)
                return
            print('appending temp result to df_write')
            self.df_write.append(df)
            print('appending temp result to sheet_name')
            self.sheet_name.append(_config['sheet_name'])
            print('append html_str_list to visual_write')
            self.visual_write.append(graphs)
            print('-----------------------------------')

        
