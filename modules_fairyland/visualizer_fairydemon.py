import os
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

class BaseGragh:
    def gen(self, df:pd.DataFrame):
        """
        处理数据的方法，需要在子类中具体实现。
        :param data: 需要处理的数据
        :return: 处理后的数据
        """
        raise NotImplementedError("Process method must be defined in subclass")
    
    def draw_pie(self, **kwargs):
        if 'data' not in kwargs:
            raise ValueError("data must be provided")
        
        from pyecharts.charts import Pie
        from pyecharts import options as opts
        data = kwargs.get('data')
        pie = Pie(opts.InitOpts(width="1000px",height="500px"))
        # pie = Pie(opts.InitOpts(
        #     width="900px",
        #     height="500px"))
        print("Base graph draw_pie")
        print(kwargs.get('title'))
        title = kwargs.get('title')
        print(kwargs.get('subtitle'))
        subtitle = kwargs.get('subtitle')
        # pie.set_global_opts(title_opts=opts.TitleOpts(
        #     title="老哥示范",
        #     title_link="http://pyecharts.org/",
        #     subtitle="老哥太强了",
        #     pos_top="10%"
        # ))
        # set subtitle
        pie.add("消费过的推送玩家人数", data, rosetype="radius", radius=["30%", "55%"])
        pie.set_series_opts(tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"))
        pie.set_global_opts(legend_opts=opts.LegendOpts(type_="scroll", pos_right="2%", orient="vertical"))
        pie.set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c} - {d}%"))
        # put legend on the below pos
        # pie.set_global_opts(legend_opts=opts.LegendOpts(pos_bottom="right"))
        # pie.set_global_opts(legend_opts=opts.LegendOpts(orient="vertical", pos_top="15%", pos_right="2%"))
        pie.set_global_opts(legend_opts=opts.LegendOpts(False))
        pie.set_global_opts(opts.TitleOpts(title=subtitle + '-' + title))
        # pie.set_global_opts(opts.TitleOpts(title='大你好', subtitle='小你好'))
    #     pie.set_global_opts(legend_opts=opts.LegendOpts(
    #        type_='scroll',
    #        selected_mode='multiple',
    #        is_show=True,
    #        pos_left='',
    #        pos_right='10%',
    #        pos_top='20%',
    #        pos_bottom='',
    #        orient='vertical',
    #        align='left',
    #        padding=5,
    #        item_gap=20,
    #        item_width=15,
    #        item_height=15,
    #        inactive_color='blue',
    #        legend_icon='circle'
    #   ))
        # put tile on the left pos
        # pie.set_global_opts(title_opts=opts.TitleOpts(pos_left="left"))
        return pie
 
class BAR_task_4_top10_chargetimes_n_players(BaseGragh):
    def gen(self, df):
        from pyecharts.charts import Pie, Bar
        from pyecharts import options as opts
        from pyecharts.commons.utils import JsCode
        from pyecharts.globals import ThemeType

        # df = pd.read_csv(
        #     'processed_task_4.csv',
        #     header=0,
        #     encoding='utf-8'
        # )
        df.to_excel('df.xlsx', index=True)
        # keep column with its name not containing '比例'
        df = df[[col for col in df.columns if '比例' not in col]]

        graphs = []
        for date in df['日期'].unique():
            _df = df[df['日期'] == date]
            _df.reset_index(drop=True, inplace=True)               
            _df_sum = _df.drop(columns=['日期', '充值次数']).sum(axis=1)
            _df_value = _df.drop(columns=['日期', '充值次数'])
            bar = \
                Bar(init_opts=opts.InitOpts(width="1420px", height="1420px", theme=ThemeType.LIGHT)) \
                .add_xaxis(['充值1次', '充值2次', '充值3次', '充值4次', '充值5次', '充值6次', '充值7次', '充值8次', '充值9次', '充值10次']) \
            
            for col in _df_value.columns:
                x = _df['充值次数']
                # y = [{'value': v, 'percent': v/s} for v, s in zip(_df_value[col], _df_sum)]
                y = [{'value': v/s, 'num': v} for v, s in zip(_df_value[col], _df_sum)]
                bar.add_yaxis(col, y, stack="stack1", category_gap="50%")
           
            bar.set_series_opts(
                label_opts=opts.LabelOpts(
                    position="right",
                    formatter=JsCode(
                        "function(x){return Number(x.data.value * 100).toFixed() + '%';}"
                    ),
                )
            )
            bar.set_global_opts(legend_opts=opts.LegendOpts(type_="scroll", pos_right="2%", pos_top="30%", orient="vertical"))
            bar.set_global_opts(tooltip_opts=opts.TooltipOpts(
                formatter=JsCode(
                "function(x){console.log(x); return x.seriesName + '---' + Number(x.data.num).toFixed() + '人';}"
                )
            ), title_opts=opts.TitleOpts(title="{} 用户付费频次分析（日报）".format(date)))
            graphs.append(bar)
            # bar.render("stack_bar_percent.html")
        return graphs

class PIE_task_3_top20_n_players(BaseGragh):
    def _draw_pie(self, **kwargs):
        # from kwargs extract title, subtitle, data
        print('PIE_task_3_top20_n_players _draw_pie')
        title = kwargs.get('title')
        # subtitle = kwargs.get('subtitle')
        data = kwargs.get('data')
        print('data')
        print(data)
        # print(title)
        # print(subtitle)
        # print(data)
        # select data column 日期, 商品名称, 充值人数, save as df
        df = data[['日期', '商品名称', '充值人数']].reset_index(drop=True)

        # create df for each 日期 by iteration
        from pyecharts.charts import Page
        from pyecharts import options as opts
        # page = Page(init_opts=opts.InitOpts(height="1000px", width="1000px"))

        # page = Page()
        graphs = []
        # pies = []
        for date in df['日期'].unique():
            df_date = df[df['日期'] == date]
            # sort by 充值人数
            df_date = df_date.sort_values(by='充值人数', ascending=False)
            # # get top 20
            # df_date = df_date.head(20)
            # draw pie
            df_date = df_date[['商品名称', '充值人数']]
            df_date = [list(z) for z in zip(df_date['商品名称'], df_date['充值人数'])]
            # page.add(super().draw_pie(title=title, subtitle=date, data=df_date))
            graphs.append(super().draw_pie(title=title, subtitle=date, data=df_date))
        # pie = self.draw_pie(title=title, subtitle=subtitle, data=data)
        print('graphs inside _draw_pie {}'.format(graphs))
        return graphs

    def gen(self, df):
        print('PIE_task_3_top20_n_players gen')
        pies = self._draw_pie(title='用户商品购买间隔行为分析TOP20（日报）', subtitle='玩家人数', data=df)
        return pies
    
class VisualGraph:
    def gen(self, PARAMS):
        raise NotImplementedError("Process method must be defined in subclass")
    
class visual_5_1_1(VisualGraph):
    def __init__(self) -> None:
        print('visual_5_1_1 class created')

    def gen(self, PARAMS):
        page = Page()
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
        # select _df when dt == '20240226'
        _df = _df[_df['dt'] == PARAMS['dt_T']]
        data = _df[['func_name'
                    ,'orders'
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

        data['func_name_with_cost'] = addtion.apply(lambda row: f"{row['func_name']}|{row['cost']}", axis=1)

        pie = Pie()
        pie_data = list(data[['func_name_with_cost', 'orders']].itertuples(index=False, name=None))
        pie_data
        pie.add(
            series_name="",
            data_pair=pie_data,
            radius=["30%", "75%"],
            center=["50%", "50%"],
            rosetype="radius",
        )
        pie.set_global_opts(
            title_opts=opts.TitleOpts(title=f"仙魔大陆各模块数据大盘兑换订单占比 - 日期：{PARAMS['dt_T']}"),
            legend_opts=opts.LegendOpts(is_show=False),
        )
        pie.set_series_opts(tooltip_opts=opts.TooltipOpts(formatter=JsCode("""
        function(params) {
            var [func_name, cost] = params.name.split('|');
            return 
                '订单数：' + params.value
                +'  金额：' + cost;
        }
        """)))
        pie.set_series_opts(label_opts=opts.LabelOpts(formatter=JsCode("""
        function(params) {
            var [func_name, cost] = params.name.split('|');
            return func_name + 
                '占比：' + params.percent.toFixed(1) + '%' + 
                '; 金额：' + cost;
        }
        """)))
        # add to page
        page.add(pie)

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
            title_opts=ComponentTitleOpts(title=f"仙魔大陆各模块与前日对比明细 - 日期：{PARAMS['dt_T']}")
        )
        # add to page
        page.add(table)

        # table 2
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
            ,'orders'
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

        df_20240226 = data[data['dt'] == PARAMS['dt_T']].sort_values(by='orders', ascending=False)
        # df_20240225 = data[data['dt'] == 20240225]

        df_20240226_sorted = df_20240226.sort_values(by='orders', ascending=False)
        sub_func_names_sorted = df_20240226_sorted['sub_func_name'].tolist()
        # df_20240225_sorted = df_20240225.set_index('sub_func_name').loc[sub_func_names_sorted].reset_index()

        bar = Bar()
        bar.add_xaxis(sub_func_names_sorted)
        bar.add_yaxis(f"{PARAMS['dt_T']}", df_20240226_sorted['orders'].tolist())
        # bar.add_yaxis("20240225 昨日订单数", df_20240225_sorted['orders'].tolist())

        bar.set_global_opts(
            title_opts=opts.TitleOpts(title=f"仙魔大陆各礼包模块兑换订单数 - 日期：{PARAMS['dt_T']}"),
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=45)),
            yaxis_opts=opts.AxisOpts(name="兑换订单数"),
            legend_opts=opts.LegendOpts(pos_left='80%', pos_top='0%')
            # datazoom_opts=[opts.DataZoomOpts()],
        )

        # add to page
        page.add(bar)

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
            title_opts=ComponentTitleOpts(title=f"仙魔大陆各礼包模块与前日对比明细 - 日期：{PARAMS['dt_T']}")
        )
        # add to page
        page.add(table)

        # table 3
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
            ,'orders'
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

        df_20240226 = data[data['dt'] == PARAMS['dt_T']]

        bar = Bar()
        bar.add_xaxis(df_20240226['goods'].tolist())
        bar.add_yaxis(f"全部礼包", df_20240226['orders'].tolist())
        # bar.add_yaxis("20240225 昨日订单数", df_20240225_sorted['orders'].tolist())

        bar.set_global_opts(
            title_opts=opts.TitleOpts(title=f"仙魔大陆所有模块TOP10礼包 兑换订单数 - 日期：{PARAMS['dt_T']}"),
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=15)),
            yaxis_opts=opts.AxisOpts(name="兑换订单数"),
            legend_opts=opts.LegendOpts(pos_left='80%', pos_top='0%')
            # datazoom_opts=[opts.DataZoomOpts()],
        )

        # add to page
        page.add(bar)

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
            title_opts=ComponentTitleOpts(title=f"仙魔大陆所有模块各礼包TOP10礼包 与前日对比明细 - 日期：{PARAMS['dt_T']}")
        )
        # add to page
        page.add(table)


        # table 4
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
            if func_name == '境界推送礼包' or func_name == '贺岁礼包':
                continue
            data = df_20240226[df_20240226['func_name'] == func_name][[
                'func_name'
                ,'goods'
                ,'orders'
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
            bar = Bar()
            bar.add_xaxis(data['goods'].tolist())
            bar.add_yaxis(f"{func_name}", data['orders'].tolist())

            bar.set_global_opts(
                title_opts=opts.TitleOpts(title=f"[{func_name}]: 礼包TOP10礼包兑换订单数 - 日期：{PARAMS['dt_T']}"),
                xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=15)),
                yaxis_opts=opts.AxisOpts(name="兑换订单数"),
                legend_opts=opts.LegendOpts(pos_left='80%', pos_top='0%')
                # datazoom_opts=[opts.DataZoomOpts()],
            )

            # add to page
            page.add(bar)

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
            headers= ['模块名称', '礼包名称ID', '兑换订单数', '与前日对比', '与前日对比占比', '金额', '与前日对比 ', '与前日对比占比 ']
            rows = table_data.values.tolist()

            table = Table()

            table.add(headers, rows)
            table.set_global_opts(
                title_opts=ComponentTitleOpts(title=f"[{func_name}]: 与前日对比数据补充")
            )
            # add to page
            page.add(table)

        return page


class Visualizer:
    def __init__(self, config, root_path) -> None:
        self.config = config
        self.root_path = root_path
        self.vis_dict = config['visual']
        self.end_dt = config['end_dt']
        self.temp_save_path = config['connector']['temp_save_path']
        assert self.end_dt is not None, 'end_dt is not existed'
    
    def run(self):
        for k, v in self.vis_dict.items():
            config = self.vis_dict[k]

            dir = os.path.join(self.root_path, self.temp_save_path, self.end_dt)
            output_dir = os.path.join(self.root_path, self.temp_save_path, self.end_dt, config['output_file'])

            PARAMS = {file.split('.')[0]: os.path.join(dir, file) for file in config['files']}
            PARAMS.update({
                'dt_T': int(self.end_dt)
            })
            print(f"PARAMS: {PARAMS}")

            graph = self._create_graph_instance(config['template'])
            page = graph.gen(PARAMS)

            page.render(output_dir)

    def _create_graph_instance(self, graph_name):
        # 使用 globals() 或者专门的策略注册字典来查找对应的类
        if graph_name in globals():
            strategy_class = globals()[graph_name]
            if issubclass(strategy_class, VisualGraph):
                return strategy_class()
        raise ValueError(f"Unknown graph: {graph_name}")
    
    def _read_date(self):
        file = os.path.join(self.root_path, self.temp_save_path)+'/'+self.read_file_name
        print('file: {}'.format(file))
        df = pd.read_csv(file,header=0,encoding='utf-8')
        print(df.head(10))
        return df
    
    def _save_graph(self, page):
        file = os.path.join(self.root_path, self.temp_save_path)+'/'+self.save_file_name
        # page.render(file)

    def gen(self, df):
        try:
            if self.config['read_from_file']:
                print("read from file")
                df = self._read_date()
            graphs = self.graph.gen(df)
            print('graphs: {}'.format(graphs))
            if self.save_to_file:
                self._save_graph(graphs)
        except Exception as e:
            print('Process failed.')
            if not self.read_from_file:
                print('Visual: 读取flag为false，但是读取数据失败，可能是数据文件不存在。')
            print(e)
        return graphs
    # graph are dynamic

