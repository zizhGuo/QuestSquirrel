from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font, Border, Side, Alignment
import pandas as pd

from base_transform import BaseTransform
from base_transform import df2ws, arg_test_outer, insert_row

import logging

class LevelFailureTransform(BaseTransform):
    def __init__(self):
        super(BaseTransform, self).__init__()

    @BaseTransform.merge_cells
    def post_edit(self, *args):
        pass

    def run(self, ws, df, start_row, dt = '日期', level = '境界等级', target_column = '玩家名单'):
        ret_row = super(LevelFailureTransform, self).run(ws, df, start_row)
        self.post_edit(ws, df, start_row, dt= dt, level= level, target_column= target_column)
        return ret_row

class SpiritFailureTransform(LevelFailureTransform):
    def __init__(self):
        super(LevelFailureTransform, self).__init__()
    
    def run(self, ws, df, start_row):
        ret_row = super(SpiritFailureTransform, self).run(ws, df, start_row, 
                                                    dt = '日期', 
                                                    level = '神识等级',
                                                    target_column = '玩家名单')
        return ret_row


class DFdescriptor:
    """作为df的描述器，用于描述df的基本信息
    """
    def __init__():
        pass

    def __set__():
        pass

    def __get__():
        pass


class BreakthruFailureIntervalConsumption(BaseTransform):
    groupby_target_test_case = [
        (['日期', '境界等级', '等级名称', '失败次数', '礼包去重序列'], '玩家名单')
    ]

    groupby_target = [
        (['失败次数', '礼包去重序列'], '玩家名单'),
        (['失败次数', '玩家名单'], '礼包去重序列')
    ]
    
    col = ['日期', '境界等级', '等级名称', '玩家名单', '失败次数', '第几次失败', '失败时消耗材料', '礼包去重序列', '礼包', '购买订单数', '累计订单金额']
    
    def __init__(self):
        super(BaseTransform, self).__init__()
        
    @BaseTransform.drop_columns
    def drop(self, *args):
        pass

    @BaseTransform.select_columns
    def select(self, df, columns_dict):
        pass

    @insert_row
    def insert(self):
        pass

    @df2ws
    @BaseTransform.merge_cells_general
    def edit_ws(self, ws, df, start_row, *args, **kwargs):
        print('enter edit_ws')
        pass
    
    def run(self, ws, df, start_row):
        start_row = start_row
        df = self.drop(df, ['境界等级'])
        for dt in df['日期'].unique():
            for level in df[df['日期']==dt]['等级名称'].unique():
                columns_dict = {
                    '日期': dt,
                    '等级名称': level
                }
                _df = self.select(df, columns_dict)
                _df = self.drop(_df, ['等级名称', '日期'])
                start_row = self.insert(ws, start_row, f'日期：{dt} 境界等级: {level}')
                ret_row = self.edit_ws(ws, _df, start_row, 
                                       groupby_target = self.groupby_target,
                                       alignment = 'general,center'
                )
                start_row = ret_row
        return ret_row
