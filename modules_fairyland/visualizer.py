import os
import pandas as pd

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
    

class Visualizer:
    def __init__(self, config, root_path) -> None:
        self.config = config
        self.root_path = root_path
        self.graph = self._create_graph_instance(config['graph_name'])
        print(self.graph)
        self.read_from_file = config['read_from_file']
        self.save_to_file = config['save_to_file']
        self.temp_save_path = config['temp_save_path']
        self.read_file_name = config['read_file_name']
        self.save_file_name = config['save_file_name']

    def _create_graph_instance(self, graph_name):
        # 使用 globals() 或者专门的策略注册字典来查找对应的类
        if graph_name in globals():
            strategy_class = globals()[graph_name]
            if issubclass(strategy_class, BaseGragh):
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

