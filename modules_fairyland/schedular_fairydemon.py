import os
import sys
print(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'multiphases')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'multiphases')))

from modules_fairyland.connector import HiveConnector
from modules_fairyland.query import QueryManager
from modules_fairyland.processor import DataProcessor
from modules_fairyland.visualizer import Visualizer
from modules_fairyland.report import ReportGenerator
from modules_fairyland.email import Email
from modules_fairyland.flag import Flag

import time

from abc import abstractmethod
import threading
from queue import SimpleQueue
import traceback

import multiprocessing

import logging

logger_scheduler = logging.getLogger('SCHEDULER Module logger')
logger_scheduler.setLevel(logging.DEBUG)

logger_hivetask = logging.getLogger('HIVE TASK logger')
logger_hivetask.setLevel(logging.DEBUG)

logger_sparktask = logging.getLogger('SPARK TASK logger')
logger_sparktask.setLevel(logging.DEBUG)


console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

logger_scheduler.addHandler(console_handler)
logger_hivetask.addHandler(console_handler)
logger_sparktask.addHandler(console_handler)

MAX_CONCURRENT_TASKS = 5


"""_summary_
更新task schedule调度方式

1、运行querys save dataframes
    
"""

class TaskScheduler:
    def __init__(self, config, root_path, param_dt, module) -> None:
        # self.tasks = []
        # self.connector = HiveConnector(config, root_path, module)
        self.config = config
        self.root_path = root_path
        self.param_dt = param_dt
        self.module = module

        self.DBUG_LOGS = config.get('DEBUG_LOGS', 0)

        # TODO init lock, semaphore
        self.semaphore = threading.Semaphore(MAX_CONCURRENT_TASKS)
        self.task_queue = SimpleQueue()
        self.lock = threading.Lock()

    def run_tasks(self):
        threads = []
        logger_scheduler.debug('TaskScheduler: run tasks entered')
        start = time.time()
        
        for sub_task, _sub_config in self.config[self.module.query_module].items():
            query_tasks_type = _sub_config.get('type', 'hive')
            # if query_tasks_type == 'spark' or  query_tasks_type == 'hive':
            if query_tasks_type == 'spark':
                self.execute_report(self.config, self.root_path, self.param_dt, self.module, sub_task, _sub_config)
                # thread = threading.Thread(target=self.execute_report, args=(self.config, self.root_path, self.param_dt, self.module, sub_task))
                # logger_scheduler.debug(f'thread created: {thread}')
                # logger_scheduler.debug(f'current sub_task created: {sub_task}')
                # logger_scheduler.debug('----------------------')
                # threads.append(thread)
                # thread.start()

        # for sub_task, _sub_config in self.config[self.module.query_module].items():
        #     query_tasks_type = _sub_config.get('type', 'hive')
        #     # if query_tasks_type == 'spark' or  query_tasks_type == 'hive':
        #     if query_tasks_type == 'spark':
        #         thread = threading.Thread(target=self.execute_report, args=(self.config, self.root_path, self.param_dt, self.module, sub_task))
        #         logger_scheduler.debug(f'thread created: {thread}')
        #         logger_scheduler.debug(f'current sub_task created: {sub_task}')
        #         logger_scheduler.debug('----------------------')
        #         threads.append(thread)
        #         thread.start()

        # for thread in threads:
        #     thread.join()

        threads = []
        for sub_task, _sub_config in self.config[self.module.query_module].items():
            query_tasks_type = _sub_config.get('type', 'hive')
            if query_tasks_type == 'hive':
                thread = threading.Thread(target=self.execute_report, args=(self.config, self.root_path, self.param_dt, self.module, sub_task, _sub_config))
                logger_scheduler.debug(f'thread created: {thread}')
                logger_scheduler.debug(f'current sub_task created: {sub_task}')
                threads.append(thread)
                thread.start()

        for thread in threads:
            thread.join()
        
        end = time.time()
        print('spent time: ', end-start)

    def execute_report(self, config, root_path, param_dt, module, sub_task, _sub_config):
        logger_scheduler.debug('execute_report, TaskHive creating')
        self.type = self.config[self.module.query_module][sub_task].get('type', 'hive')
        print(f'type: {self.type}')
        
        if self.type == 'hive':
            report = TaskHive(config, root_path, param_dt, module, sub_task
                          , self.task_queue, self.lock, self.semaphore, self.DBUG_LOGS)
            report.run_querys()
        elif self.type == 'spark':
            report = TaskSpark(config, root_path, param_dt, module, sub_task, _sub_config)
            report.run_querys_multiprocess()

    # def execute_report_multiprocess(self, config, root_path, param_dt, module, sub_task):
    #     logger_scheduler.debug('execute_report, TaskSpark creating')
    #     report = TaskSpark(config, root_path, param_dt, module, sub_task)
    #     report.run_querys_multiprocess()

class Task:
    """
        abstract class to define interface
    """
    def __init__(self):
        pass
    
    @abstractmethod
    def run_querys(self):
        pass

class TaskHive(Task):
    """
        sub_query level: task
        factory mode:
            querymanager to process sql template
            connector to run query and save dataframes
    """
    def __init__(self, config, root_path, param_dt, module, sub_task, task_queue, lock, semaphore, DEBUG_LOGS):
        super().__init__()
        self.config = config
        self.root_path = root_path
        self.param_dt = param_dt
        self.module = module

        # newly added threads params
        self.task_queue = task_queue
        self.lock = lock
        self.semaphore = semaphore

        self.DBUG_LOGS = DEBUG_LOGS

        self.config['end_dt'] = param_dt['end_dt']
        self.config['start_dt'] = param_dt['start_dt']

        assert self.config is not None, "Config is None"

        self.query_manager = QueryManager(self.config, self.root_path, self.param_dt, module, sub_task)
        # self.query_manager.print_query_runs()
        self.quries = self.query_manager.query_runs

        # self.connector = HiveConnector(self.config, self.root_path, module)
        self.template_names = self.query_manager.template_names
        self.tables = self.config[module.query_module][sub_task]["tables"]
        self.tasks_num = int(self.config[module.query_module][sub_task]["tasks_num"])
        assert self.tasks_num == len(self.tables), "Tasks num is not equal to tables num"
        assert self.tasks_num == len(self.template_names), "Tasks num is not equal to template names num"
        if self.config[module.query_module][sub_task].get("flags") is not None:
            self.flags = self.config[module.query_module][sub_task]["flags"]
        else:
            self.flags = [1 for _ in range(self.tasks_num)]
        self.temp_save_path = self.config[module.query_module][sub_task]["temp_save_path"]

    def run_querys(self):
        logger_hivetask.debug('TaskHive: run_querys entered.')
        threads = []

        for i in range(self.tasks_num):
            if self.flags[i] == 0:
                continue
            logger_hivetask.debug('TaskHive: run_querys loop entered.')
            print(f'running querys: {i}')
            print(self.quries[self.template_names[i]])
            thread = threading.Thread(target=self.worker, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
            print('running querys done')
            print('-----------------------------------')
            print('-----------------------------------')
            print('-----------------------------------')
            print('-----------------------------------')
            print('-----------------------------------')
            print('-----------------------------------')

    def worker(self, i):
        # try:
        #     from random import randint
        #     time.sleep(randint(1,5))
        # except Exception as e:
        #     print('init connector failed: ', e)
        # else:
        try:
            print(f'Hive query started: {i}')
            print(self.quries[self.template_names[i]])
            with self.semaphore:
                # random time sleep 1-5s
                from random import randint
                time.sleep(randint(1,5))
                connector = HiveConnector(self.config, self.root_path, self.module)
                result = connector.query_and_save(self.quries, self.template_names, i, self.tables, self.flags, self.temp_save_path, self.DBUG_LOGS)
            print('Hive running querys done')
            print('-----------------------------------')
            print('-----------------------------------')
            print('-----------------------------------')
            print('-----------------------------------')
            print('-----------------------------------')
            print('-----------------------------------')
        except Exception as e:
            print(f'Hive Query: {i} run querys failed.')
            print(e)
        else:
            print('Hive worker result: {}'.format(result))
            # finally:
                # connector.close()
                # logger.debug('thread worker: close connection from the inside no matter what')
                # connector.query_result_save()
                # result = connector.query_and_save()
            
class TaskSpark(Task):
    def __init__(self, config, root_path, param_dt, module, sub_task, _sub_config):
        """
            params initialization:
                config: dict
                root_path: str
                param_dt: dict
                module: module
                sub_task: str
        """
        super().__init__()
        self._set_logger()
        self.num_processes = 4
        self.stage_config = _sub_config
        try:
            self._gen_args(config, root_path, param_dt, module, sub_task)
            self._gen_task_params()
        except Exception as e:
            self.logger.debug(f'Spark Task nitialization failed: ', e)
            print(traceback.format_exc())
            self.init_success = 0
        else:
            self.init_success = 1

    def _set_logger(self):
        self.logger = logging.getLogger('SPARK TASK logger')
        self.logger.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def _gen_args(self, config, root_path, param_dt, module, sub_task):
        """
            generate args for spark
        """
        self.config = config
        self.root_path = root_path
        self.param_dt = param_dt
        self.module = module
        # self.config['end_dt'] = param_dt['end_dt']
        # self.config['start_dt'] = param_dt['start_dt']
        self.end_dt = param_dt['end_dt']
        self.start_dt = param_dt['start_dt']
        self.tables = self.config[module.query_module][sub_task]["tables"]

        # self.flags = []
        # for i, table in tables:
        #     self.flags

    def _gen_task_params(self):
        self.public_task_params = {}
        self.public_task_params.update({'end_dt': self.end_dt})
        self.public_task_params.update({'start_dt': self.start_dt})
        self.public_task_params.update(self.stage_config)
        # print(f'self.stage_config: {self.stage_config}')
        # print(f'self.public_task_params: {self.public_task_params}')


    def _get_save_file(self, temp_save_path, file):
        return os.path.join(self.root_path, temp_save_path, self.end_dt, file)
    
    def _get_base_dir(self, temp_save_path):
        return os.path.join(self.root_path, temp_save_path, self.end_dt)

    def run_querys_multiprocess(self):
        """多进程
        """
        jobs = []
        # params = [(i, table) for i, table in enumerate(self.tables)]:

        for i, table in enumerate(self.tables):
            try:
                # entire spark tasks init failed, break all
                if self.init_success == 0: 
                    break  # 
                
                # type check
                flag = table[0]
                temp_save_path = table[1]
                mod = table[2]
                obj = table[3]
                save_file = table[4]
                
                file = self._get_save_file(temp_save_path, save_file)
                base_dir = self._get_base_dir(temp_save_path)
                # 初始化参数
                params = {}
                params.update(self.public_task_params)
                params.update({'others': table[5:]})
                params.update({'save_file': file})
                params.update({'base_dir': base_dir})
                params.update({'root_path': self.root_path})
                params.update({'template': save_file.split('.')[0]})
                self.logger.debug(f'Spark task params: {params}')

            except Exception as e:
                self.logger.debug('A Spark task type check failed: ', e)
                print(traceback.format_exc())
            else:
                jobs.append((i, mod, obj, params))
        try:
            with multiprocessing.Pool(processes=self.num_processes) as pool:
                results = [pool.apply_async(self.worker_multiprocessing, args=(i, mod, obj, params)) for i, mod, obj, params in jobs]
                
                for result in results:
                    result.wait()
                
                all_successful = all(result.get() for result in results)
                if all_successful:
                    logging.info("All jobs completed successfully.")
                else:
                    logging.error("Some jobs failed.")
        except Exception as e:
            self.logger.debug('Spark Task Multiprocess failed: ', e)
            print(traceback.format_exc())
        


    def run_querys(self):
        """
            control flow: multithreading 
            For loop in len(tasks_num)
        """
        threads = []
        for i, table in enumerate(self.tables):
            try:
                # entire spark tasks init failed, break all
                if self.init_success == 0: 
                    break  # 
                
                # type check
                flag = table[0]
                temp_save_path = table[1]
                mod = table[2]
                obj = table[3]
                save_file = table[4]
                
                file = self._get_save_file(temp_save_path, save_file)
                base_dir = self._get_base_dir(temp_save_path)
                # 初始化参数
                params = {}
                params.update(self.public_task_params)
                params.update({'others': table[5:]})
                params.update({'save_file': file})
                params.update({'base_dir': base_dir})
                self.logger.debug(f'Spark task params: {params}')

            except Exception as e:
                self.logger.debug('A Spark task type check failed: ', e)
                print(traceback.format_exc())
            else:
                if flag == 1:
                    # create thread
                    # start thread
                    thread = threading.Thread(target=self.worker, args=(i, mod, obj, params))
                    threads.append(thread)
                    self.logger.debug(f'thread {thread} added to threads {threads}')
                    thread.start()
                    self.logger.debug(f'thread {thread} started')

        
        for thread in threads:
            thread.join()

    def worker_multiprocessing(self, i, mod, obj, params):
        try:
            # create spec task obj
            task_obj = self._create_instance(mod, obj)(params) # ? TODO 参数传入
        except Exception as e:
            self.logger.debug(f'Spark Task Worker create instance failed: ', e)
        else:
            try:
                # call general query function
                print(f'Spark query started: {i}')
                task_obj.query_and_save()
                print('Spark running querys done')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
            except Exception as e:
                print(f'Spark Query: {i} run querys failed.')
                print(e)
                print(traceback.format_exc())
            else:
                print('Spark Task Worker Job finished')

    def worker(self, i, mod, obj, params):
        """
            initialize the spark session
            create customized
            
        """
        try:
            # create spec task obj
            task_obj = self._create_instance(mod, obj)(params) # ? TODO 参数传入
        except Exception as e:
            self.logger.debug(f'Spark Task Worker create instance failed: ', e)
        else:
            try:
                # call general query function
                print(f'Spark query started: {i}')
                with self.semaphore:
                    task_obj.query_and_save()
                    print(traceback.format_exc())
                print('Spark running querys done')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
            except Exception as e:
                print(f'Spark Query: {i} run querys failed.')
                print(e)
            else:
                print('Spark Task Worker Job finished')
            
        

    def _create_instance(self, mod, obj):
        """_summary_
            return the instance of the Customized class
        """
        import importlib
        module = importlib.import_module(mod)
        Class = getattr(module, obj)
        print('successfully import transform class.')
        return Class

        
