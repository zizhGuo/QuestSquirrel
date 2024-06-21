"""
此基类负责处理pyspark任务的数据处理流程

流程分为1. query 2. process 3. save

作者：gzz

更新日期: 20240604
"""

import os
import sys

import pandas as pd
import logging
import traceback
import time

from contextlib import contextmanager

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, when

print('append module path: {}'.format(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

# get parent dir
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 切面方法引用
from modules_fairyland.decorators import retry


# @contextmanager
# def managed_sparksession(spark: SparkSession):
#     try:
#         yield spark
#     finally:
#         spark.stop()

PARAMS = {
    'Envir2Doupo': {
        'app_name': 'Envir2Doupo',
        'logger_name': 'Envir2Doupo_logger'
    }
}

class QuestTaskBase:
    """
        pyspark task 基类
        :设置所有参数
        :设置专属logger
    """
    def __init__(self, params, *args, **kwargs) -> None:
        print('QuestTaskBase init entered')
        self.params = params
        if 'PARAMS' in kwargs:
            PARAMS.update(kwargs['PARAMS'])
        self._set_args()
        self._create_logger()
        self._set_default_sql()

        print('QuestTaskBase init done')
    
    @staticmethod
    def _update_PARAMS(kwargs, params):
        if 'PARAMS' in kwargs:
            params.update(kwargs['PARAMS'])
            kwargs.pop('PARAMS')
            kwargs.update({'PARAMS': params})
        else:
            kwargs.update({'PARAMS': params})

        return kwargs

    @property
    def sql(self):
        if not hasattr(self, '_sql'):
            self._sql = self._set_default_sql()
        return self._sql

    def _set_default_sql(self):
        self._sql = ""
        if self.args.get('others') and len(self.args['others']) == 2:
            if isinstance(self.args['others'][1], dict) and isinstance(self.args['others'][0], dict):
                self._sql = self._load_query()
                print('______sql________')
                print('_________________')
                print(self._sql)
                print('_________________')
                print('______end________')
        
        if self.args.get('others') and len(self.args['others']) > 2:
            if isinstance(self.args['others'][1], dict) and isinstance(self.args['others'][0], dict):
                self._sql = self._load_query()
            self._set_extra_args()
        return self._sql

    @property
    def args(self):
        if not hasattr(self, '_args') or self._args is None:
            self._args = self._set_args()
        return self._args

    def _set_args(self):
        """
            设置参数
        """
        self._args = {}
        self._args.update(self.params)
        self._args.update(PARAMS[self.__class__.__name__])
        # print(f'_set_args -> self._args: {self._args}')

    def _set_extra_args(self):
        """
            设置额外参数
            :override this method and set extra args if needed
            :额外参数使用update更新self.args中
        """
        pass

    @property
    def spark(self):
        if not hasattr(self, '_spark'):
            self._spark = self._create_or_get_spark()
        if self._spark is None:
            self._spark = self._create_or_get_spark()
        return self._spark

    @retry(max_retries=3, retry_delay=5, exception_to_check=Exception)
    def _create_or_get_spark(self):
        return SparkSession.builder \
            .config("spark.executor.instances", "2") \
            .config("spark.executor.cores", "2") \
            .appName(self.args['app_name']) \
            .enableHiveSupport() \
            .getOrCreate()
    
    @property
    def logger(self):
        if not hasattr(self, '_logger') or self._logger is None:
            self._logger = self._create_logger()
        return self._logger

    def _create_logger(self):
        self._logger = logging.getLogger(self.args['logger_name'])
        self._logger.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self._logger.addHandler(console_handler)
        return self._logger

    def _stop_spark(self):
        """
            停止spark session
        """
        self.logger.debug('_stop_spark entered')
        self.logger.debug(f'_stop_spark: self._spark: {self._spark}')
        if hasattr(self, '_spark'):
            try:
                while self._spark is not None and \
                        not self._spark.sparkContext._jsc.sc().isStopped():
                    self._spark.stop()
                    self._spark = None
                    time.sleep(1)
                    self.logger.debug('spark stopped')

            except Exception as e:
                self.logger.debug('stop spark failed')
                print(e)
                print(traceback.format_exc())
            else:
                self.logger.debug('stop spark successfull')
            finally:
                self.logger.debug('delete spark session')
                del self._spark
        else:
            self.logger.debug('no spark session to stop')
        self.logger.debug('_stop_spark done')
    
    def save_data(self, df, *args, **kwargs):
        """
            add result save function
        """
        format = 'xlsx'
        if self.args['save_file'].split('.')[1] == 'csv':
            format = 'csv'
        # self.result save
        try:
            self.logger.debug(f'try creating dir or not, just check: {self.args["base_dir"]}')
            os.makedirs(self.args['base_dir'], exist_ok=True)
        except Exception as e:
            self.logger.debug(f'create dir failed: {self.args["base_dir"]}')
            print(e)
            print(traceback.format_exc())
        else:
            self.logger.debug(f'create dir done: {self.args["base_dir"]}')
            try:
                self.logger.debug(f'try saving data: {self.args["save_file"]}')
                if format == 'csv':
                    df.to_csv(self.args['save_file'], index=False)
                else:
                    df.to_excel(self.args['save_file'], index=False, engine='openpyxl')
            except Exception as e:
                self.logger.debug(f'save data failed: {self.args["save_file"]}')
                print(e)
                print(traceback.format_exc())
            else:
                self.logger.debug(f'save data done: {self.args["save_file"]}')
    
    def _load_query(self):
        def _process_query(query):
            import re
            query = query.replace('\n', ' ')
            query = re.sub(r'\s+', ' ', query) 
            return query

        self.logger.debug(f'_load_query entered')
        _dir = self.args['others'][0]
        self.args.update(self.args['others'][1])
        req_cat = _dir['req_cat']
        req_iter = _dir['req_iter']
        req_status = _dir['req_status']
        template_file = self.args['template']+'.sql'
        if self.args.get('template_type') and self.args['template_type'] == 'stable':
            template_file = os.path.join(self.args['root_path'],"templates",req_cat,req_iter,req_status,template_file)
        elif self.args.get('template_type') and self.args['template_type'] == 'test_batch':
            template_file = os.path.join(self.args['root_path'],"templates",req_cat,req_iter,'test_batch',template_file)
        elif self.args.get('template_type') and self.args['template_type'] == 'test':
            template_file = os.path.join(self.args['root_path'],"templates",req_cat,req_iter,'test',template_file)
        else:
            template_file = os.path.join(self.args['root_path'],"templates",req_cat,req_iter,req_status, template_file)
        
        try:
            self.logger.debug(f'try load query: {template_file}')
            with open(template_file, 'r') as f:
                sql = f.read()
        except Exception as e:
            self.logger.debug(f'load query failed: {template_file}')
            print(e)
            print(traceback.format_exc())
        else:
            self.logger.debug(f'load query done: {template_file}')
            return _process_query(sql.format(**self.args))    

    @retry(max_retries=3, retry_delay=5, exception_to_check=Exception)
    def query_v1(self, *args, **kwargs):
        """
        默认处理query流程v1
        """
        self.logger.debug(f'started query_v1')
        result = self.spark.sql(self.sql)
        result.show()
        return result.toPandas()

    def process_v1(self, result, *args, **kwargs):
        """
        默认transform流程v1
        """
        self.logger.debug(f'started process v1')
        if self.args.get('column_name'):
            if isinstance(self.args['column_name'], list):
                result.columns = self.args['column_name']
        return result

    def query_and_process_v1(self, query, process, *args, **kwargs):
        """
        默认处理流程v1
        :param query: query方法
        :param process: process方法
        """
        try:
            result = query(*args, **kwargs)
        except Exception as e:
            self.logger.debug('query failed')
            print(e)
            return 'failed'
        else:
            self.logger.debug('query success, start processing')
            try:
                df = process(result, *args, **kwargs)
            except Exception as e:
                self.logger.debug(f'process failed, class name: {self.__class__.__name__}')
                print(e)
                return 'failed'
            else:
                self.logger.debug('process success, start saving')
                try:
                    self.save_data(df)
                except Exception as e:
                    self.logger.debug('save failed')
                    print(e)
                    return 'failed'
        finally:
            # self.logger.debug(f'query_and_process_v1: finally check _spark: {self._spark}')
            # self.logger.debug(f'query_and_process_v1: finally sc._jsc.sc().isStopped(): {self.spark.sparkContext._jsc.sc().isStopped()}')
            self._stop_spark()
            self.logger.debug(f'{self.__class__.__name__} done.')

    def query_and_process_v2(self, query, process, *args, **kwargs):
        """
        处理流程v2
        适配pyspark单机环境下load parquet文件
        与v1区别: 取消调用spark stop, 维持temp view
        :param query: query方法
        :param process: process方法
        """
        try:
            result = query(*args, **kwargs)
        except Exception as e:
            self.logger.debug('query failed')
            print(e)
            return 'failed'
        else:
            self.logger.debug('query success, start processing')
            try:
                df = process(result, *args, **kwargs)
            except Exception as e:
                self.logger.debug(f'process failed, class name: {self.__class__.__name__}')
                print(e)
                return 'failed'
            else:
                self.logger.debug('process success, start saving')
                try:
                    self.save_data(df)
                except Exception as e:
                    self.logger.debug('save failed')
                    print(e)
                    return 'failed'
        finally:
            # self.logger.debug(f'query_and_process_v1: finally check _spark: {self._spark}')
            # self.logger.debug(f'query_and_process_v1: finally sc._jsc.sc().isStopped(): {self.spark.sparkContext._jsc.sc().isStopped()}')
            # self._stop_spark()
            self.logger.debug(f'{self.__class__.__name__} done.')

    def query_and_save(self):
        """
            入口方法
            :重写 不调用super实例方法,实现v2,v3....
        """
        # pass
        self.query_and_process_v1(self.query_v1, self.process_v1)



sample_yaml = """

abc:
    - [1, 2, '3', {
        'a': 1, 
        'b': 2}
    , 4]

"""

if __name__ == '__main__':
    import yaml
    params = yaml.load(sample_yaml, Loader=yaml.FullLoader)
    print(params)
    print(params['abc'])
    print(params['abc'][0])
    print(params['abc'][0][3])
    print(params['abc'][0][3]['b'])
    print(params['abc'][0][4])