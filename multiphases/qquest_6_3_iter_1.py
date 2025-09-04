"""
此类负责处理pyspark YJBY 渔场 斗破类需求

作者：郭子谆

更新日期: 20240605
"""

import os
import sys

import pandas as pd

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, when

print('append module path: {}'.format(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from modules_fairyland.decorators import retry
from qquest_base import QuestTaskBase

PARAMS = {
    'Envir2Doupo': {
        'app_name': 'Envir2Doupo',
        'logger_name': 'Envir2Doupo_logger',
        'column_name': ['日期', 
                        'level','level_name','rn',
                        '名称', '经历过对应等级总人数', 
                        '类型', '操作总次数', '操作总人数'],
        'drop_column': ['level', 'level_name', 'rn']
    },
    'Envir3Doupo': {
        'app_name': 'Envir3Doupo',
        'logger_name': 'Envir3Doupo_logger',
        'column_name': ['日期' ,'总游戏人数' ,'与前一日对比', 
                        '人均游戏时长（分钟）' ,'与前一日对比' 
                        ,'总消耗金币' ,'与前一日对比' ,'净分', 
                        '与前一日对比'],
    },
    'Envir4Doupo': {
        'app_name': 'Envir4Doupo',
        'logger_name': 'Envir4Doupo_logger',
        'column_name': ['日期' , '渔场名称' 
                        ,'渔场游戏人数' ,'与前一日对比' 
                        , '人均游戏时长（分钟）' ,'与前一日对比' 
                        ,'总消耗金币' ,'消耗金币占比' ,'与前一日对比' 
                        ,'净分', '与前一日对比'],
    },
    'EnvirLevelFailure': {
        'app_name': 'EnvirLevelFailure',
        'logger_name': 'EnvirLevelFailure_logger',
        'column_name': ['日期', '等级', '等级名称', '失败次数', '失败人数'],
        'drop_column': ['等级']
    },
    'BreakthruLevelNonOperate': {
        'app_name': 'BreakthruLevelNonOperate',
        'logger_name': 'BreakthruLevelNonOperate_logger',
        'column_name': ['日期', '等级', '未操作总人数', '活跃未操作人数','平均道具库存'],
    }
}

class Envir2Doupo(QuestTaskBase):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

    def process_v1(self, result: pd.DataFrame):
        self.logger.debug(f'started process v1')
        result.columns = self.args['column_name']
        result.drop(columns=self.args['drop_column'], inplace=True)
        return result

    def query_and_save(self):
        super().query_and_save()
        # self.query_and_process_v1(self.query_v3, self.process_v1)

class Envir3Doupo(QuestTaskBase):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

    def query_and_save(self):
        super().query_and_save()
        # self.query_and_process_v1(self.query_v3, self.process_v1)

class Envir4Doupo(QuestTaskBase):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

    def query_and_save(self):
        super().query_and_save()
        # self.query_and_process_v1(self.query_v3, self.process_v1)

class EnvirLevelFailure(Envir2Doupo):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

    def query_and_save(self):
        super().query_and_save()
        # self.query_and_process_v1(self.query_v3, self.process_v1)

class BreakthruLevelNonOperate(QuestTaskBase):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

    def query_and_save(self):
        super().query_and_save()
        # self.query_and_process_v1(self.query_v3, self.process_v1)
