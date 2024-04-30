from base_transform import BaseTransform

from utils_dateframe import drop_columns, select_columns
from utils_worksheet import df2ws, merge_cells_general, insert_row
from quest_6_1_iter_1 import Envir4
from quest_5_2_iter_1 import LevelFailureTransform, BreakthruLevelFailureIntervalConsumption


class BreakthruLevelNonOperate(Envir4):
    """
        6-1-iter1-生态4表-去掉渔场id列
        Inheret from Envir4
        STEPS:
        1. drop_columns
        2. df2ws
    """
    def __init__(self):
        self.drop_columns = ['等级']
        
    def run(self, ws, df, start_row):
        return super().process_v1(ws, df, start_row)

class LevelFailureTransform(LevelFailureTransform):
    """
        6-2-iter1-境界失败表
        Inheret from LevelFailureTransform
        STEPS:
        1. df2ws
        2. merge_cells_general
    """
    def __init__(self):
        self.groupby_target = [
            (['日期', '等级名称'], '玩家名单')
    ]

    def run(self, ws, df, start_row):
        return super().process_v1(ws, df, start_row, groupby_target=self.groupby_target)

class BreakthruPacificFailureIntervalConsumption(BreakthruLevelFailureIntervalConsumption):
    """
        6-2-iter1-表2.2.1 突破失败间隙礼包购买转化
    """
    def __init__(self) -> None:
        self.groupby_target = [
            (['失败次数', '礼包去重序列'], '玩家名单'),
            (['失败次数', '玩家名单'], '礼包去重序列')
        ]
        self.args = {
            'outer_col': '日期',
            'inner_col': '等级名称',
            'drop_col': ['清除等级'],
            'drop_cols': ['等级名称', '日期'],
            'insert': '{outer_col}: {dt} {inner_col}: {level}',
            'groupby_target': [
                    (['失败次数', '礼包去重序列'], '玩家名单'),
                    (['失败次数', '玩家名单'], '礼包去重序列')
            ],
            'alignment': 'general,center',
        }

    def process_v1(self, ws, df, start_row, *args, **kwargs):
        return super().process_v3(ws, df, start_row, *args, **kwargs)
    
    def run(self, ws, df, start_row):
        return self.process_v1(ws, df, start_row)