from base_transform import BaseTransform

from utils_dateframe import drop_columns, select_columns
from utils_worksheet import df2ws, merge_cells_general, insert_row
from quest_6_1_iter_1 import Envir4
from quest_5_2_iter_1 import LevelFailureTransform, BreakthruLevelFailureIntervalConsumption, VipFisheryDistribution as VIP


class BreakthruLevelNonOperate(Envir4):
    """
        6-1-iter1-生态4表-去掉渔场id列
        Inheret from Envir4
        STEPS:
        1. drop_columns
        2. df2ws
    """
    def __init__(self, *args, **kwargs):
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
    def __init__(self, *args, **kwargs):
        self.groupby_target = [
            (['日期', '等级名称'], '玩家名单')
    ]

    def run(self, ws, df, start_row):
        return super().process_v1(ws, df, start_row, groupby_target=self.groupby_target)

class BreakthruPacificFailureIntervalConsumption(BreakthruLevelFailureIntervalConsumption):
    """
        6-2-iter1-表2.2.1 突破失败间隙礼包购买转化
    """
    def __init__(self, *args, **kwargs) -> None:
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

class EnvirChurnFisheryActivePacific:
    """
        6-2-iter1-表2.2.2 流失渔场活跃度
    """
    def __init__(self, *args, **kwargs) -> None:
        self.args = {
            'flag_column_name': '渔场名称',
            'target_col': '远古部落',
            'replace_val': '\\',
            'drop_column_name': '渔场id',
            'border_style':{
                'regular': True
            },
            'info': '注：\"活跃用户未在对应渔场中游戏用户数\" 指代\"前一日在该渔场游戏，当日未在该渔场游戏的活跃用户数\"'
        }

    from utils_dateframe import drop_columns, modify_cell_matched_v3
    from utils_worksheet import df2ws

    @drop_columns
    def prior_edit(self, *args, **kwargs):
        pass
    
    @insert_row
    def insert(self, *args, **kwargs):
        pass
    @df2ws
    def on_edit(self, *args, **kwargs):
        pass

    @modify_cell_matched_v3
    def replace_cell(self, *args, **kwargs):
        pass

    def process_v1(self, ws, df, start_row, *args, **kwargs):
        dt = df[self.args['drop_column_name']][0]
        df = self.prior_edit(df, [self.args['drop_column_name']])
        df = df.astype(str)
        df = self.replace_cell(df, self.args['target_col'], replaced_val = self.args['replace_val'])

        # start_row = self.insert(ws, start_row, f'日期：{dt}')
        ret_row = self.on_edit(ws, df, start_row, **self.args['border_style'])
        ret_row = self.insert(ws, ret_row, self.args['info'])
        return ret_row
    
    def run(self, ws, df, start_row):
        return self.process_v1(ws, df, start_row)
    
class VipFisheryDistribution(VIP):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.args.update(
            {
            'sum_columns': ['所有渔场都玩玩家数', '只常规渔场玩家数', '只玩太平洋玩家数']
            ,'sum_new_column_name': '合计（人数）'
            ,'percent_columns': ['所有渔场都玩占比', '只玩常规渔场占比', '只玩太平洋渔场占比']
            ,'percent_new_column_name': ['所有渔场都玩占比', '只玩常规渔场占比', '只玩太平洋渔场占比']
            ,'remaining_columns': ['日期', 'VIP']
            ,'modify_dict':{
                'VIP': 'str'
            }
        }
        )
    def run(self, ws, df, start_row):
        return super().run(ws, df, start_row)
