"""
此模块用于实现任务5_1的第一次迭代的商城图示
"""
import os

from abc import abstractmethod

import pandas as pd
import numpy as np
from pyecharts import options as opts
from pyecharts.charts import Grid, Pie, Sankey, Page, Tab, Bar
from pyecharts.commons.utils import JsCode
from pyecharts.globals import ThemeType
from pyecharts.components import Table
from pyecharts.commons.utils import JsCode
# import ComponentTitleOpts
from pyecharts.options import ComponentTitleOpts

import logging


class BaseVisual:
    def __init__(self):
        super().__init__()
        self.name = 'Base'
    
    @abstractmethod
    def plot(self):
        pass

    @abstractmethod
    def _gen_params(self, vargs):
        pass

    def _gen_params(self, vargs):
        base_dir = vargs[-1]['base_dir']
        assert len(vargs[3]) > 0, "No source tables provided"
        params = {file.split('.')[0]: os.path.join(base_dir, file) for file in vargs[3]}
        # params = {print(file) for file in vargs[3]}
        params.update({'dt_T': int(vargs[-1]['end_dt'])})
        return params

# class ScriptVisualizer(BaseVisual):
#     def __init__(self, params):
#         super().__init__(params)
#         self.name = 'Script'

#     @abstractmethod
#     def load_template(self):
#         print('load_template')
    
# class RegularVisualizer(BaseVisual):
#     def __init__(self, params):
#         super().__init__(params)
#         self.name = 'Regular'

#     @abstractmethod
#     def load_data(self):
#         print('load_data')
    
#     @abstractmethod
#     def plot(self):
#         print('plot')
    
#     def run(self):
#         self.load_data()
#         self.plot()

class FairydemonMall(BaseVisual):
    def __init__(self, vargs):
        # print('entered FairydemonMall initialized')
        # print('FairydemonMall -> vargs[-1]', vargs[-1])
        # print('FairydemonMall -> vargs', vargs)
        super().__init__()
        self.name = 'Fairy'
        self.params = self._gen_params(vargs)
        para = {
            'title': '捕鱼炸翻天-仙魔大陆',
            '境界推送礼包': '境界推送礼包',
            '贺岁礼包': '贺岁礼包'
        }
        self.params.update(para)
        print(self.params)

    def _gen_params(self, vargs):
        # _dir = os.path.join(vargs[-1]['root_path'], vargs[-1]['temp_save_path'], vargs[-1]['end_dt'])
        base_dir = vargs[-1]['base_dir']
        assert len(vargs[3]) > 0, "No source tables provided"
        params = {file.split('.')[0]: os.path.join(base_dir, file) for file in vargs[3]}
        # params = {print(file) for file in vargs[3]}
        params.update({'dt_T': int(vargs[-1]['end_dt'])})
        return params
    
    def gen(self, PARAMS):
        chunk_list = []
        # page = Page(page_title='仙魔大陆商城图示')

        # task 1

        _df = pd.read_excel(PARAMS['dws_mall_task1'])
        _df = _df.fillna(0)
        _df['func_name'] = _df['func_name'].astype(str)
        print(_df.dtypes)
        print(_df)
        # rename columns
        _df = _df.rename(columns={
            'n_non_welfare_exchange_orders': 'orders'
            ,'non_welfare_exchange_cost': 'cost'
            ,'non_welfare_exchange_orders_ratio': 'orders_ratio'
            ,'non_welfare_exchange_cost_ratio': 'cost_ratio'
            ,'n_non_welfare_exchange_orders_diff': 'orders_diff'
            ,'non_welfare_exchange_cost_diff': 'cost_diff'
            ,'non_welfare_exchange_orders_ratio_diff': 'orders_diff_ratio'
            ,'non_welfare_exchange_cost_ratio_diff': 'cost_diff_ratio'
        })
        _df
        # select _df when dt == '20240226'
        _df = _df[_df['dt'] == PARAMS['dt_T']].sort_values(by='cost', ascending=False)
        data = _df[['func_name'
                    ,'cost'
                    ]]
        addtion = _df[['func_name'
                    ,'orders'
                    ,'cost'
                    ,'orders_ratio'
                    ,'cost_ratio'
                    ,'orders_diff'
                    ,'cost_diff'
                    ,'orders_diff_ratio'
                    ,'cost_diff_ratio'
                    ]]
        data['func_name_with_orders'] = addtion.apply(lambda row: f"{row['func_name']}|{row['orders']}", axis=1)
        # print(data)

        pie = Pie()
        pie_data = list(data[['func_name_with_orders', 'cost']].itertuples(index=False, name=None))
        print('pie data: ', pie_data)
        pie.add(
            series_name="",
            data_pair=pie_data,
            radius=["30%", "75%"],
            center=["50%", "50%"],
            rosetype="radius",
        )
        pie.set_global_opts(
            title_opts=opts.TitleOpts(title=f"{PARAMS['title']}各模块数据大盘兑换金额占比 - 日期：{PARAMS['dt_T']}"),
            legend_opts=opts.LegendOpts(is_show=False),
        )
        pie.set_series_opts(tooltip_opts=opts.TooltipOpts(formatter=JsCode("""
        function(params) {
            var [func_name, orders] = params.name.split('|');
            return 
                '订单数：' + orders
                +'  金额：' + params.value;
        }
        """)))
        pie.set_series_opts(label_opts=opts.LabelOpts(formatter=JsCode("""
        function(params) {
            var [func_name, orders] = params.name.split('|');
            return func_name + 
                '占比：' + params.percent.toFixed(1) + '%' + 
                '; 金额：' + params.value;
        }
        """)))
        pie.render('task1_pie.html')
        # add to page
        chunk_list.append(pie)


        table_data = addtion[[
            'func_name'
            ,'orders'
            ,'orders_diff'
            ,'orders_diff_ratio'
            ,'cost'
            ,'cost_diff'
            ,'cost_diff_ratio'
        ]]

        # table_data['sort_group'] = table_data['cost_diff'].apply(lambda x: 1 if x < 0 else (2 if x > 0 else 3))
        # table_data = table_data.sort_values(by=['sort_group', 'cost_diff'], key=lambda x: x if x.name == 'sort_group' else abs(x), ascending=[True, False])
        # table_data.drop('sort_group', axis=1, inplace=True)

        headers = table_data.columns.tolist()
        headers= ['模块名称', '兑换订单数', '与前日对比', '与前日对比占比', '金额', '与前日对比 ', '与前日对比占比 ']
        table_data['sort_helper'] = table_data['cost_diff'] == 0
        table_data = table_data.sort_values(by=['sort_helper', 'cost_diff'], key=lambda x: x, ascending=[True, True])
        table_data.drop('sort_helper', axis=1, inplace=True)

        # convert 'orders',  'orders_diff' into int, no digits after dot
        table_data['orders'] = table_data['orders'].round(0).astype(int)
        table_data['orders_diff'] = table_data['orders_diff'].round(0).astype(int)
        table_data['cost_diff'] = table_data['cost_diff'].round(0).astype(int)
        # table_data['orders_diff_ratio'] = table_data['cost_diff'].round(2).astype(int)
        # table_data['cost_diff_ratio'] = table_data['cost_diff'].round(2).astype(int)
        print(table_data['orders'])
        # table_data = table_data.astype(str)

        rows = table_data.values.tolist()

        table = Table()
        table.add(headers, rows)
        # set legend position
        table.set_global_opts(
            title_opts=ComponentTitleOpts(title=f"{PARAMS['title']}各模块与前日对比明细 - 日期：{PARAMS['dt_T']}")
        )
        # add to page
        chunk_list.append(table)


        # task 2

        _df = pd.read_excel(PARAMS['dws_mall_task2'])
        _df = _df.fillna(0)
        _df['func_name'] = _df['func_name'].astype(str)
        print(_df.dtypes)
        _df = _df.rename(columns={
            'n_non_welfare_exchange_orders': 'orders'
            ,'non_welfare_exchange_cost': 'cost'
            ,'non_welfare_exchange_orders_ratio': 'orders_ratio'
            ,'non_welfare_exchange_cost_ratio': 'cost_ratio'
            ,'n_non_welfare_exchange_orders_diff': 'orders_diff'
            ,'non_welfare_exchange_cost_diff': 'cost_diff'
            ,'non_welfare_exchange_orders_ratio_diff': 'orders_diff_ratio'
            ,'non_welfare_exchange_cost_ratio_diff': 'cost_diff_ratio'
        })
        data = _df[[
            'dt'
            ,'func_name'
            ,'sub_func_name'
            ,'cost'
        ]]

        addition = _df[[
            'dt'
            ,'sub_func_name'
            ,'orders'
            ,'orders_diff'
            ,'orders_diff_ratio'
            ,'cost'
            ,'cost_diff'
            ,'cost_diff_ratio'
        ]]

        df_20240226 = data[data['dt'] == PARAMS['dt_T']].sort_values(by='cost', ascending=False)
        # df_20240225 = data[data['dt'] == 20240225]

        df_20240226_sorted = df_20240226.sort_values(by='cost', ascending=False)
        sub_func_names_sorted = df_20240226_sorted['sub_func_name'].tolist()
        # df_20240225_sorted = df_20240225.set_index('sub_func_name').loc[sub_func_names_sorted].reset_index()

        bar = Bar()
        bar.add_xaxis(sub_func_names_sorted)
        bar.add_yaxis(f"{PARAMS['dt_T']}", df_20240226_sorted['cost'].tolist())
        # bar.add_yaxis("20240225 昨日订单数", df_20240225_sorted['orders'].tolist())

        bar.set_global_opts(
            title_opts=opts.TitleOpts(title=f"{PARAMS['title']}各礼包模块兑换金额 - 日期：{PARAMS['dt_T']}"),
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=45)),
            yaxis_opts=opts.AxisOpts(name="兑换金额"),
            legend_opts=opts.LegendOpts(pos_left='80%', pos_top='0%')
            # datazoom_opts=[opts.DataZoomOpts()],
        )

        bar.render("bar_chart_comparison.html")
        # add to page
        chunk_list.append(bar)


        table_data = addition[addition['dt'] == PARAMS['dt_T']][[
            'sub_func_name'
            ,'orders'
            ,'orders_diff'
            ,'orders_diff_ratio'
            ,'cost'
            ,'cost_diff'
            ,'cost_diff_ratio'
        ]]
        table_data['orders_diff'] = table_data['orders_diff'].round(0).astype(int)
        table_data['cost_diff'] = table_data['cost_diff'].round(0).astype(int)
        table_data['sort_helper'] = table_data['cost_diff'] == 0
        table_data = table_data.sort_values(by=['sort_helper', 'cost_diff'], key=lambda x: x, ascending=[True, True])
        table_data.drop('sort_helper', axis=1, inplace=True)
        headers = table_data.columns.tolist()
        headers= ['礼包类型', '兑换订单数', '与前日对比', '与前日对比占比', '金额', '与前日对比 ', '与前日对比占比 ']
        rows = table_data.values.tolist()

        table = Table()

        table.add(headers, rows)
        table.set_global_opts(
            title_opts=ComponentTitleOpts(title=f"{PARAMS['title']}各礼包模块与前日对比明细 - 日期：{PARAMS['dt_T']}")
        )
        # add to page
        chunk_list.append(table)


        # task 3

        _df = pd.read_excel(PARAMS['dws_mall_task3'])
        _df = _df.fillna(0)
        _df['sub_func_name'] = _df['sub_func_name'].astype(str)
        print(_df.dtypes)

        _df = _df.rename(columns={
            'dt': 'dt'
            ,'rn': 'rn'
            ,'goods': 'goods'
            ,'costcount': 'costcount'
            ,'sub_func_name': 'sub_func_name'
            ,'n_non_welfare_exchange_orders': 'orders'
            ,'non_welfare_exchange_cost': 'cost'
            ,'non_welfare_exchange_orders_ratio': 'orders_ratio'
            ,'non_welfare_exchange_cost_ratio': 'cost_ratio'
            ,'n_non_welfare_exchange_orders_diff': 'orders_diff'
            ,'non_welfare_exchange_cost_diff': 'cost_diff'
            ,'non_welfare_exchange_orders_ratio_diff': 'orders_diff_ratio'
            ,'non_welfare_exchange_cost_ratio_diff': 'cost_diff_ratio'
        })

        data = _df[[
            'dt'
            ,'sub_func_name'
            ,'goods'
            ,'cost'
        ]]

        addition = _df[[
            'dt'
            ,'sub_func_name'
            ,'goods'
            ,'orders'
            ,'orders_diff'
            ,'orders_diff_ratio'
            ,'cost'
            ,'cost_diff'
            ,'cost_diff_ratio'
        ]]

        df_20240226 = data[data['dt'] == PARAMS['dt_T']].sort_values(by='cost', ascending=False)

        bar = Bar()
        bar.add_xaxis(df_20240226['goods'].tolist())
        bar.add_yaxis(f"全部礼包", df_20240226['cost'].tolist())
        # bar.add_yaxis("20240225 昨日订单数", df_20240225_sorted['orders'].tolist())

        bar.set_global_opts(
            title_opts=opts.TitleOpts(title=f"{PARAMS['title']}所有模块TOP10礼包 兑换金额 - 日期：{PARAMS['dt_T']}"),
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=15)),
            yaxis_opts=opts.AxisOpts(name="兑换金额"),
            legend_opts=opts.LegendOpts(pos_left='80%', pos_top='0%')
            # datazoom_opts=[opts.DataZoomOpts()],
        )

        # bar.render("task3_bar.html")
        # add to page
        chunk_list.append(bar)

        table_data = addition[addition['dt'] == PARAMS['dt_T']][[
            'goods'
            ,'orders'
            ,'orders_diff'
            ,'orders_diff_ratio'
            ,'cost'
            ,'cost_diff'
            ,'cost_diff_ratio'
        ]]
        table_data['orders_diff'] = table_data['orders_diff'].round(0).astype(int)
        table_data['cost_diff'] = table_data['cost_diff'].round(0).astype(int)
        table_data['sort_helper'] = table_data['cost_diff'] == 0
        table_data = table_data.sort_values(by=['sort_helper', 'cost_diff'], key=lambda x: x, ascending=[True, True])
        table_data.drop('sort_helper', axis=1, inplace=True)

        headers = table_data.columns.tolist()
        headers= ['礼包名称ID', '兑换订单数', '与前日对比', '与前日对比占比', '金额', '与前日对比 ', '与前日对比占比 ']
        rows = table_data.values.tolist()

        table = Table()

        table.add(headers, rows)
        table.set_global_opts(
            title_opts=ComponentTitleOpts(title=f"{PARAMS['title']}所有模块各礼包TOP10礼包 与前日对比明细 - 日期：{PARAMS['dt_T']}")
        )
        # add to page
        chunk_list.append(table)


        # task 4

        _df = pd.read_excel(PARAMS['dws_mall_task4'])
        _df = _df.fillna(0)
        _df['func_name'] = _df['func_name'].astype(str)
        _df['func_name'] = _df['func_name'].astype(str)
        print(_df.dtypes)

        _df = _df.rename(columns={
            'dt': 'dt'
            ,'rn': 'rn'
            ,'goods': 'goods'
            ,'costcount': 'costcount'
            ,'func_name': 'func_name'
            ,'n_non_welfare_exchange_orders': 'orders'
            # ,'n_non_welfare_exchange_orders_original': 'orders_original'
            # ,'n_non_welfare_exchange_orders_discount': 'orders_discount'
            ,'non_welfare_exchange_cost': 'cost'
            ,'non_welfare_exchange_orders_ratio': 'orders_ratio'
            ,'non_welfare_exchange_cost_ratio': 'cost_ratio'
            ,'n_non_welfare_exchange_orders_diff': 'orders_diff'
            ,'non_welfare_exchange_cost_diff': 'cost_diff'
            ,'non_welfare_exchange_orders_ratio_diff': 'orders_diff_ratio'
            ,'non_welfare_exchange_cost_ratio_diff': 'cost_diff_ratio'
        })
        df_20240226 = _df[_df['dt'] == PARAMS['dt_T']]


        # iterate through values in _df['dt']
        func_names = df_20240226['func_name'].unique()
        # sort dates descending
        # func_names = np.sort(func_names)[::-1]



        for func_name in func_names:
            if func_name == PARAMS['境界推送礼包'] or func_name == PARAMS['贺岁礼包']:
                continue
            data = df_20240226[df_20240226['func_name'] == func_name][[
                'func_name'
                ,'goods'
                ,'cost'
            ]]
            addition = df_20240226[df_20240226['func_name'] == func_name][[
                'func_name'
                ,'goods'
                ,'orders'
                ,'orders_diff'
                ,'orders_diff_ratio'
                ,'cost'
                ,'cost_diff'
                ,'cost_diff_ratio'
            ]]
            data = data.sort_values(by = 'cost', ascending=False)

            bar = Bar()
            bar.add_xaxis(data['goods'].tolist())
            bar.add_yaxis(f"{func_name}", data['cost'].tolist())

            bar.set_global_opts(
                title_opts=opts.TitleOpts(title=f"[{func_name}]: 礼包TOP10礼包兑换金额 - 日期：{PARAMS['dt_T']}"),
                xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=15)),
                yaxis_opts=opts.AxisOpts(name="兑换金额"),
                legend_opts=opts.LegendOpts(pos_left='80%', pos_top='0%')
                # datazoom_opts=[opts.DataZoomOpts()],
            )

            # bar.render(f"task4_bar_{func_name}.html")
            # add to page
            chunk_list.append(bar)

            table_data = addition[[
                'func_name'
                ,'goods'
                ,'orders'
                ,'orders_diff'
                ,'orders_diff_ratio'
                ,'cost'
                ,'cost_diff'
                ,'cost_diff_ratio'
            ]]

            table_data['orders_diff'] = table_data['orders_diff'].round(0).astype(int)
            table_data['cost_diff'] = table_data['cost_diff'].round(0).astype(int)
            table_data['sort_helper'] = table_data['cost_diff'] == 0
            table_data = table_data.sort_values(by=['sort_helper', 'cost_diff'], key=lambda x: x, ascending=[True, True])
            table_data.drop('sort_helper', axis=1, inplace=True)

            headers = table_data.columns.tolist()
            print(table_data)

            headers= ['模块名称', '礼包名称ID', '兑换订单数', '与前日对比', '与前日对比占比', '金额', '与前日对比 ', '与前日对比占比 ']
            rows = table_data.values.tolist()

            table = Table()

            table.add(headers, rows)
            table.set_global_opts(
                title_opts=ComponentTitleOpts(title=f"[{func_name}]: 与前日对比数据补充")
            )

            # add to page
            chunk_list.append(table)

        return chunk_list

    def plot_v1(self):
        return self.gen(self.params)
    
    def plot(self):
        # super().plot()
        chunk_list = self.plot_v1()
        if not isinstance(chunk_list, list):
            raise TypeError('plot_v1() should return a list')
        return chunk_list



yaml_str = """
visual:
  vis1:
    output_file: 'quest_fairydemon_visual.html'
    temp_save_path: "data/quest_fairydemon"
    scripts:
        - [0, 1, 'quest_5_1_iter_1', 'Test', 'dws_mall_task1.xlsx.xlsx', 'dws_mall_task2.xlsx.xlsx', 'dws_mall_task1.xlsx.xlsx', 'dws_mall_task1.xlsx.xlsx']
        - [1, 1, 'quest_5_1_iter_1', 'Test2', 'dws_mall_task1.xlsx.xlsx']
    regular:
      [1, 1, 'quest_5_1_iter_1', 'Test2', 'dws_mall_task1.xlsx']
"""

import yaml

# using yaml package to load the yaml string
config = yaml.load(yaml_str, Loader=yaml.FullLoader)
# logging.info(f'config: {config}')
# print(f'config: {config}')
# print(config.get('visual').get('vis1').get('scripts'))


# for item in config.get('visual').get('vis1').get('scripts'):
#     print(item)

# t2 = Test2(params=None)
# t2.plot()