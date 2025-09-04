import os
import sys

import pandas as pd

import logging
import traceback

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, when

print('append module path: {}'.format(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from modules_fairyland.decorators import retry
# from qquest_base import QuestTaskBase
from multiphases.qquest_local import QuestLocal

PARAMS = {
    'EnvirWaveTriggerVip': {
        'app_name': 'EnvirWaveTriggerVip',
        'logger_name': 'EnvirWaveTriggerVip_logger',
        'column_name': ['日期', '当日总活跃用户数', 'VIP等级', '当日活跃用户数', '当日触发波动用户数','占比', '当日未触发波动用户数', '占比'],
    }
    ,'PostWaveTriggerCharge': {
        'app_name': 'PostWaveTriggerCharge',
        'logger_name': 'PostWaveTriggerCharge_logger',
        'column_name': ['日期','商品名称','单价','订单笔数','订单金额'],
    }    
    ,'PostNonWaveTriggerCharge': {
        'app_name': 'PostNonWaveTriggerCharge',
        'logger_name': 'PostNonWaveTriggerCharge_logger',
        'column_name': ['日期','商品名称','单价','订单笔数','订单金额'],
    }    
    ,'WaveTriggerTypeNPlayers': {
        'app_name': 'WaveTriggerTypeNPlayers',
        'logger_name': 'WaveTriggerTypeNPlayers_logger',
        'column_name': ['日期','触发波动用户总数','获得奖励类型','涉及人数','占比'],
    }
    ,'EnvirWaveTriggerCharge': {
        'app_name': 'EnvirWaveTriggerCharge',
        'logger_name': 'EnvirWaveTriggerCharge_logger',
        'column_name': ['日期','当日总活跃用户数','当日触发波动用户数','触发后未充值人数','触发波动后充值人数','充值总金额','ARPPU'],
    }
    ,'Envir2ndWaveTriggerCharge': {
        'app_name': 'Envir2ndWaveTriggerCharge',
        'logger_name': 'Envir2ndWaveTriggerCharge_logger',
        'column_name': ['日期','首次触发波动时间','首次触发波动人数','未再次触发波动人数','再次触发波动人数','触发波动后充值人数','充值总金额','充值金额与上次波动后的充值对比','ARPPU','ARPPU与上次的对比'],
    }
    ,'Envir2ndNonWaveTriggerValue': {
        'app_name': 'Envir2ndNonWaveTriggerValue',
        'logger_name': 'Envir2ndNonWaveTriggerValue_logger',
        'column_name': ['日期','首次触发波动时间','未再次触发波动总人数','当前波动值范围','编号','涉及人数'],
    }
    ,'Envir2ndNonWaveTriggerGoldStock': {
        'app_name': 'Envir2ndNonWaveTriggerGoldStock',
        'logger_name': 'Envir2ndNonWaveTriggerGoldStock_logger',
        'column_name': ['日期','首次触发波动时间','未再次触发波动总人数','金币库存范围','编号','涉及人数'],
    }
    ,'Envir2ndNonWaveTriggerCannon': {
        'app_name': 'Envir2ndNonWaveTriggerCannon',
        'logger_name': 'Envir2ndNonWaveTriggerCannon_logger',
        'column_name': ['日期','首次触发波动时间','未再次触发波动总人数','炮倍范围','编号','涉及人数'],
    }
    ,'Envir2ndNonWaveTriggerGamingTime': {
        'app_name': 'Envir2ndNonWaveTriggerGamingTime',
        'logger_name': 'Envir2ndNonWaveTriggerGamingTime_logger',
        'column_name': ['日期','首次触发波动时间','未再次触发波动总人数','游戏时长范围','编号','涉及人数'],
    }
    ,'EnvirNonWaveTriggerValue': {
        'app_name': 'EnvirNonWaveTriggerValue',
        'logger_name': 'EnvirNonWaveTriggerValue_logger',
        'column_name': ['日期','当日未触发波动用户总数','当前波动值范围','编号','涉及人数'],
    }
    ,'EnvirNonWaveTriggerGoldStock': {
        'app_name': 'EnvirNonWaveTriggerGoldStock',
        'logger_name': 'EnvirNonWaveTriggerGoldStock_logger',
        'column_name': ['日期','当日未触发波动用户总数','金币库存范围','编号','涉及人数'],
    }
    ,'EnvirNonWaveTriggerCannon': {
        'app_name': 'EnvirNonWaveTriggerCannon',
        'logger_name': 'EnvirNonWaveTriggerCannon_logger',
        'column_name': ['日期','当日未触发波动用户总数','炮倍范围','编号','涉及人数'],
    }
    ,'EnvirNonWaveTriggerGamingTime': {
        'app_name': 'EnvirNonWaveTriggerGamingTime',
        'logger_name': 'EnvirNonWaveTriggerGamingTime_logger',
        'column_name': ['日期','当日未触发波动用户总数','游戏时长范围','编号','涉及人数'],
    }
    ,'EnvirNonWaveTriggerCharge': {
        'app_name': 'EnvirNonWaveTriggerCharge',
        'logger_name': 'EnvirNonWaveTriggerCharge_logger',
        'column_name': ['日期','当日未触发波动用户总数','当日充值用户数','当日充值金额','人均充值金额'],
    }
    ,'Envir2ndNonWaveTrigger7DaysFollowUp': {
        'app_name': 'Envir2ndNonWaveTrigger7DaysFollowUp',
        'logger_name': 'Envir2ndNonWaveTrigger7DaysFollowUp_logger',
        'column_name': ['日期','未触发波动日期','未触发波动用户总数','Day2触发波动用户数','Day3触发波动用户数','Day4触发波动用户数','Day5触发波动用户数','Day6触发波动用户数','Day7触发波动用户数'],
    }
    ,'Envir2ndNonWaveTriggerVip': {
        'app_name': 'Envir2ndNonWaveTriggerVip',
        'logger_name': 'Envir2ndNonWaveTriggerVip_logger',
        'column_name': ['日期','未再次触发波动总人数','VIP等级','未再次触发波动人数'],
    }
}


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

class EnvirWaveTriggerCharge(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class Envir2ndWaveTriggerCharge(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class Envir2ndNonWaveTriggerValue(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class Envir2ndNonWaveTriggerGoldStock(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class Envir2ndNonWaveTriggerCannon(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class Envir2ndNonWaveTriggerGamingTime(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class EnvirNonWaveTriggerValue(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class EnvirNonWaveTriggerGoldStock(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class EnvirNonWaveTriggerCannon(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class EnvirNonWaveTriggerGamingTime(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class EnvirNonWaveTriggerCharge(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class Envir2ndNonWaveTrigger7DaysFollowUp(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

class Envir2ndNonWaveTriggerVip(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)