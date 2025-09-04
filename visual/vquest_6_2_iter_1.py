"""
此模块用于实现任务5_1的第一次迭代的商城图示
"""
from visual.vquest_5_2_iter_1 import VipFisheryDistributionVis as VFD

class VipFisheryDistributionVis(VFD):
    def __init__(self, vargs):
        super().__init__(vargs)
        self.name = 'Pacific'
        # update parents' args
        self.para.update({
            'title': '姚记捕鱼-',
            '境界推送礼包': '境界推送礼包',
            '贺岁礼包': '贺岁礼包',
            'new_col': ['日期', 'VIP', '所有渔场都玩玩家数', '只常规渔场玩家数', '只玩太平洋玩家数'],
            'drop_col': ['日期'],
            'add_col': ['总计'],
            'insert_col': self.args['sum_new_column_name'],
            'pie_flag': {'all_data': 1},
            'pie_val_col': ['所有渔场都玩玩家数', '只常规渔场玩家数', '只玩太平洋玩家数'],
            'pie_class': 'VIP',
            'pie_title': '各VIP等级渔场游戏情况分布（人数比例）'
            }
        )
        self.params.update(self.para)

        # update grand parents' args
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