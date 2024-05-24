"""
此模块用于实现任务5_1的第一次迭代的商城图示
"""
from visual.vquest_5_1_iter_1 import FairydemonMall
from visual.visual_base import BaseVisual
from transform.quest_5_2_iter_1 import VipFisheryDistribution
import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Grid, Pie, Sankey, Page, Tab, Bar
from pyecharts.commons.utils import JsCode
from pyecharts.globals import ThemeType
from pyecharts.components import Table
from pyecharts.commons.utils import JsCode
# import ComponentTitleOpts
from pyecharts.options import ComponentTitleOpts


import os

class FantasyMall(FairydemonMall):
    def __init__(self, vargs):
        super(FantasyMall, self).__init__(vargs)
        self.name = 'Fantasy'
        self.para = {
            'title': '捕鱼炸翻天-奇幻海域',
            '境界推送礼包': '境界推送礼包',
            '贺岁礼包': '贺岁礼包'
        }
        self.params.update(self.para)
    
    def plot_v1(self):
        return super().plot_v1()
    
    def plot(self):
        return self.plot_v1()
    
class VipFisheryDistributionVis(BaseVisual, VipFisheryDistribution):
    def __init__(self, vargs):
        super(VipFisheryDistributionVis, self).__init__()
        self.name = '仙魔需求2.0 表1.7 VIP'
        self.params = super()._gen_params(vargs)
        self.para = {
            'title': '捕鱼炸翻天-',
            '境界推送礼包': '境界推送礼包',
            '贺岁礼包': '贺岁礼包',
            'new_col': ['日期', 'VIP', '所有渔场都玩玩家数', '只常规渔场玩家数', '只玩仙魔玩家数'],
            'drop_col': ['日期'],
            'add_col': ['总计'],
            'insert_col': self.args['sum_new_column_name'],
            'pie_flag': {'all_data': 1},
            'pie_val_col': ['所有渔场都玩玩家数', '只常规渔场玩家数', '只玩仙魔玩家数'],
            'pie_class': 'VIP',
            'pie_title': '各VIP等级渔场游戏情况分布（人数比例）'
        }
        self.params.update(self.para)

    def read_df(self):
        df = pd.read_excel(self.params['dws_envir_fishery_vip'])
        df = df.fillna(0)
        df.columns = self.params['new_col']
        return df

    def get_df_table(self, df: pd.DataFrame):
        df = super().process_df(df)
        df = df.drop(columns=self.params['drop_col'], inplace=False)
        loc = df.columns.get_loc(self.params['insert_col'])
        df.insert(loc=loc, column=self.params['add_col'][0], value='100%')
        return df

    def get_df_pie(self, df: pd.DataFrame):
        df = super().process_df(df, **self.params['pie_flag'])
        return df
    
    def gen_table(self, df):
        """
            TODO: plot table:
                add one column
                return table
        """
        headers = df.columns.tolist()
        rows = df.values.tolist()
        table = Table()
        table.add(headers = headers, rows = rows)
        table.set_global_opts(
            title_opts=ComponentTitleOpts(title=f"{self.params['title']}每日各VIP等级渔场游戏情况分布- 日期：{self.params['dt_T']}")
        )

        return [table]

    from utils_pyecharts import plot_pie

    @plot_pie
    def _gen_pie(self, *args, **kwargs):
        pass


    def gen_pie(self, df):
        print('enter gen pie')
        _param = {
            'class': self.params['pie_class'],
            'title': self.params['pie_title'],
            'dt_T': self.params['dt_T']
        }
        # df = df.iloc[:-1]
        
        # print('df: ', df)
        _pies = []
        for col in self.params['pie_val_col']:
            _param.update({'column': col})
            _param.update({'total_n_players': df.iloc[-1][col]})
            _pie = self._gen_pie(df.iloc[:-1], **_param)
            _pies.append(_pie)


        return _pies
    
    def plot_v1(self): 
        df = self.read_df()
        df_table = self.get_df_table(df)
        df_pie = self.get_df_pie(df)
        # TODO plot

        return self.gen_table(df_table) + self.gen_pie(df_pie)
    
    def plot_v2(self): 
        """
            修改后只保留饼图
        """
        df = self.read_df()
        df_pie = self.get_df_pie(df)
        # TODO plot
        return  self.gen_pie(df_pie)   

    def plot(self):
        return self.plot_v2()