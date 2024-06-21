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
    'EnvirWaveTriggerVip': {
        'app_name': 'EnvirWaveTriggerVip',
        'logger_name': 'EnvirWaveTriggerVip_logger',
        'column_name': ['日期', '当日总活跃用户数', 'VIP等级', '当日活跃用户数', '当日触发波动用户数','占比', '当日未触发波动用户数', '占比'],
        'end_dt': '20240620',
        'start_dt': '20240610'
    }
    ,'PostWaveTriggerCharge': {
        'app_name': 'PostWaveTriggerCharge',
        'logger_name': 'PostWaveTriggerCharge_logger',
        'column_name': ['日期','商品名称','单价','订单笔数','订单金额'],
        'end_dt': '20240620',
        'start_dt': '20240610'
    }    
    ,'PostNonWaveTriggerCharge': {
        'app_name': 'PostNonWaveTriggerCharge',
        'logger_name': 'PostNonWaveTriggerCharge_logger',
        'column_name': ['日期','商品名称','单价','订单笔数','订单金额'],
        'end_dt': '20240620',
        'start_dt': '20240610'
    }    
    ,'WaveTriggerTypeNPlayers': {
        'app_name': 'WaveTriggerTypeNPlayers',
        'logger_name': 'WaveTriggerTypeNPlayers_logger',
        'column_name': ['日期','触发波动用户总数','获得奖励类型','涉及人数','占比'],
        'end_dt': '20240620',
        'start_dt': '20240610'
    }    
    ,'PostWaveTriggerChargeValid': {
        'app_name': 'PostWaveTriggerChargeValid',
        'logger_name': 'WPostWaveTriggerChargeValid_logger',
        'column_name': ['日期','玩家id','礼包名称和id','单价','交易价', '波动触发时间', '购买时间']
    }    
    ,'PostNonWaveTriggerChargeValid': {
        'app_name': 'PostNonWaveTriggerChargeValid',
        'logger_name': 'PostNonWaveTriggerChargeValid_logger',
        'column_name': ['日期','玩家id','礼包名称和id','单价','交易价', '购买时间']
    }    
    ,'WaveTriggerTypeNPlayersValid': {
        'app_name': 'WaveTriggerTypeNPlayersValid',
        'logger_name': 'WaveTriggerTypeNPlayersValid_logger',
        'column_name': ['日期','玩家id','打死boss奖励类型', '波动触发时间']
    }
}


class QuestLocal(QuestTaskBase):
    def __init__(self, params, *args, **kwargs) -> None:
        super().__init__(params, *args, **kwargs)

    def _create_or_get_spark(self):
        print('overwrite _create_or_get_spark')
        spark = SparkSession.builder \
            .appName("Local SparkSession") \
            .master("local[*]") \
            .getOrCreate()
        spark.sql("show tables;").show()
        return spark

    def query_and_save(self):
        super().query_and_process_v2(super().query_v1, super().process_v1)

class EnvirWaveTriggerVip(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class PostWaveTriggerCharge(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class PostNonWaveTriggerCharge(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class WaveTriggerTypeNPlayers(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class PostWaveTriggerChargeValid(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class PostNonWaveTriggerChargeValid(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class WaveTriggerTypeNPlayersValid(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)
