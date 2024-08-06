import os
import sys
print(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'multiphases')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'multiphases')))

import asyncio
from concurrent.futures import ProcessPoolExecutor


from modules_fairyland.async_scheduler import tracker, producer, consumer, sync_timed

from modules_fairyland.logger import BaseLogger

from pyspark.sql import SparkSession

from modules_fairyland.async_tasks import Tasks, Task
from modules_fairyland.utils import get_subdirectories, create_instance
import time
import random

import traceback

import logging

import copy

# logger_scheduler = logging.getLogger('SCHEDULER Module logger')
# logger_scheduler.setLevel(logging.DEBUG)

# logger_hivetask = logging.getLogger('HIVE TASK logger')
# logger_hivetask.setLevel(logging.DEBUG)

# logger_sparktask = logging.getLogger('SPARK TASK logger')
# logger_sparktask.setLevel(logging.DEBUG)


# console_handler = logging.StreamHandler()
# console_handler.setLevel(logging.DEBUG)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# console_handler.setFormatter(formatter)

# logger_scheduler.addHandler(console_handler)
# logger_hivetask.addHandler(console_handler)
# logger_sparktask.addHandler(console_handler)

MAX_CONCURRENT_TASKS = 1
DEBUG = 0

"""
新版aysnc 任务调度器
适配pyspark任务

author: gzz
date: 2024/07/30
"""

class TaskManager(BaseLogger):
    _config = None
    _root_path = None
    _param_dt = None
    _module = None
    _DBUG_LOGS = 0

    def __init__(self, config, root_path, param_dt, module) -> None:
        super().__init__(DEBUG)
        self._config = config
        self._root_path = root_path
        self._param_dt = param_dt
        self._module = module
        self._DBUG_LOGS = config.get('DEBUG_LOGS', 0)

    def run(self):
        """程序主函数入口点
        """
        for _sub_task_key, _sub_config in self._config[self._module.query_module].items():
            _type = _sub_config.get('type', 'hive')

            if _type == 'spark':
                self.logger.debug('execute_report, TaskHive creating')
                _sub_task = AsyncTasksScheduler(self._config, 
                                             self._root_path, 
                                             self._param_dt, 
                                             self._module, 
                                             _sub_task_key, 
                                             _sub_config
                                             )
                _sub_task.run()
            # TODO  _type extension
            # elif _type == 'hive':

class AsyncTasksScheduler(BaseLogger):
    """异步子任务调度器, 并发执行子任务
    """
    _config = None
    _root_path = None
    _param_dt = None
    _module = None
    _stage_config = None
    _end_dt = None
    _start_dt = None
    _tables = None
    _public_task_params = None
    _init_success = 0

    _tasks = None # 任务集合init later

    def __init__(self, config, root_path, param_dt, module, sub_task, _sub_config):
        super().__init__(DEBUG)
        try:
            self._config = config
            self._root_path = root_path
            self._param_dt = param_dt
            self._module = module
            self._stage_config = _sub_config
            self._end_dt = param_dt['end_dt']
            self._start_dt = param_dt['start_dt']
            self._tables = config[module.query_module][sub_task]["tables"]
            self._public_task_params = {}
            self._public_task_params.update({'end_dt': param_dt['end_dt']})
            self._public_task_params.update({'start_dt': param_dt['start_dt']})
            self._public_task_params.update(_sub_config)
            self._tasks = Tasks()
        except Exception as e:
            self.logger.debug(f'异步子任务调度器初始化失败: ', e)
            print(traceback.format_exc())
        else:
            # 初始化 _tasks
            for i, table in enumerate(self._tables):
                try:
                    # 提取和构建参数
                    idx = table[0]
                    dependencies = table[1]
                    temp_save_path = table[2]
                    mod = table[3]
                    obj = table[4]
                    save_file = table[5]
                    others = table[6:]
                    file = os.path.join(self._root_path, temp_save_path, self._end_dt, save_file)
                    base_dir = os.path.join(self._root_path, temp_save_path, self._end_dt)

                    # self.logger.debug('')
                    # self.logger.debug(f'save_file: {save_file}')
                    # self.logger.debug('')

                    # 打包参数
                    # params = {}
                    # params.update(self._public_task_params)
                    params = copy.deepcopy(self._public_task_params)
                    params.update({'others': others})
                    params.update({'save_file': file})
                    params.update({'base_dir': base_dir})
                    params.update({'root_path': self._root_path})
                    params.update({'template': save_file.split('.')[0]})
                    # self.logger.debug(f'Spark task params: {params}')

                except Exception as e:
                    self.logger.debug('A Spark task type check failed: ', e)
                    print(traceback.format_exc())
                else:
                    self._tasks[idx] = Task(
                                        idx=idx, 
                                        mod=mod,
                                        obj=obj,
                                        params=params,
                                        dependencies=dependencies
                                        )
                    print('\n')
                    print(self._tasks[idx])
                    print('\n')
        finally:
            # self._print_tasks_params(self._tasks)
            for task_id, task in self._tasks.items():
                # print(f'task {task_id} status: {task}')
                print(f'task {task_id} | {task}')
                print('\n')

    @sync_timed
    def task_run(self, task: Task):
        """异步任务执行函数
            序列化spark context到各进程
        """
        print(f"sync_query -{task._idx}- started")
        try:
            # random sleeping seconds
            time.sleep(random.randint(2, 5))
            task.get_query_instance().query_and_save()
        except Exception as e:
            print(f"sync_query -{task._idx}- failed: {e}")
            raise e
        
    @sync_timed
    def task_run_new(self, task: Task):
        """异步任务执行函数 - 更新
            个进程自行创建spark context
        """
        print(f"sync_query -{task._idx}- started")
        try:
            # random sleeping seconds
            # time.sleep(random.randint(2, 5))
            query_instance = create_instance(task._mod, task._obj)(task._params)
            
        except Exception as e:
            print(f"创建task实例失败: sync_query -{task._idx}- failed: {e}")
            raise e
        else:
            try:
                query_instance.query_and_save()
            except Exception as e:
                print(f"实例运行query and save失败: sync_query -{task._idx}- failed: {e}")
                raise e

    async def main(self):
        """主函数
        """
        available_tasks = []
        applied_tasks = []
        sec = 1
        with ProcessPoolExecutor(max_workers=MAX_CONCURRENT_TASKS) as pool:
            t1 = asyncio.create_task(tracker(self._tasks, applied_tasks, sec))
            t2 = asyncio.create_task(producer(self._tasks, available_tasks, sec))
            t3 = asyncio.create_task(consumer(pool, available_tasks, applied_tasks, self.task_run_new, sec))
            await asyncio.sleep(0)
            await t1

    def _print_tasks(self, tasks: Tasks):
        print('----------------------------------------')
        print("打印tasks\n {}".format(tasks))
        print('----------------------------------------')
        print('\n')
    
    def _print_tasks_params(self, tasks: Tasks):
        print('----------------------------------------')
        print("打印tasks\n {}".format(tasks.print_all_tasks_params()))
        print('----------------------------------------')
        print('\n')


    def run(self):
        """入口
        """
        print('启动异步任务调度器')
        start = time.perf_counter()
        asyncio.run(self.main())
        end = time.perf_counter()
        total = end - start
        print(f"spent time {total}")

        
