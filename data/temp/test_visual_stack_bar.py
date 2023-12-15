import pandas as pd
from pyecharts.faker import Faker
import json
# from pyecharts.commons.utils import default

def draw(df, title, subtitle):
    from pyecharts.charts import Pie, Bar
    from pyecharts import options as opts

    pie = Pie()
    # set subtitle
    pie.add("消费过的推送玩家人数", df, rosetype="radius", radius=["30%", "55%"])
    pie.set_series_opts(tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"))
    pie.set_global_opts(title_opts=opts.TitleOpts(title=title, subtitle=subtitle))
    # pie.set_global_opts(legend_opts=opts.LegendOpts(type_="scroll", pos_left="80%", orient="vertical"))
    pie.set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c} - {d}%"))
    # put legend on the below pos
    # pie.set_global_opts(legend_opts=opts.LegendOpts(pos_bottom="bottom"))
    # put tile on the left pos
    # pie.set_global_opts(title_opts=opts.TitleOpts(pos_left="left"))
    return pie

def draw_stack_bar(df, title, subtitle):
    from pyecharts import options as opts
    from pyecharts.charts import Bar
    c = (
    Bar()
    .add_xaxis(Faker.choose())
    .add_yaxis("商家A", Faker.values(), stack="stack1")
    .add_yaxis("商家B", Faker.values(), stack="stack1")
    .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    .set_global_opts(title_opts=opts.TitleOpts(title="Bar-堆叠数据（全部）"))
    .render("bar_stack0.html")
)


def main_test_faker_data_structure():
    from pyecharts.charts import Pie, Bar
    from pyecharts import options as opts
    from pyecharts.commons.utils import JsCode
    from pyecharts.globals import ThemeType

    print(type(Faker.choose()))
    print(Faker.choose())
    print(type(Faker.values()))
    print(Faker.values())
    df = pd.read_csv(
        'processed_task_4.csv',
        header=0,
        encoding='utf-8'
    )
    df.to_excel('df.xlsx', index=True)
    # keep column with its name not containing '比例'
    df = df[[col for col in df.columns if '比例' not in col]]
    for date in df['日期'].unique():
        _df = df[df['日期'] == date]
        _df.reset_index(drop=True, inplace=True)
        
        # print(_df.columns)
        # remove column 日期 充值次数
        # sum row wise
        # print(_df.drop(columns=['日期', '充值次数']).sum(axis=1))
        # _df_sum = _df.drop(columns=['日期', '充值次数']).sum(axis=1).to_excel('df_sum_{}.xlsx'.format(date), index=True)
        # for _df each row, index from 0 to 20
        # print(_df)
        # print(_df['充值次数'].to_list())
        # for i in range(1, 21):
        #     # if i not exist in _df column 充值次数, then add a row with row 0
        #     if i not in _df['充值次数'].to_list():
        #         print('found one: {} not in the list'.format(i))
        #         # add new row
        #         new_row = ([date, i] + [0 for i in range(0, 16)])
        #         print("new row = {}".format(new_row))
        #         print("old row = {}".format(_df.loc[i]))
        #         _df.loc[i] = new_row
                
        #         print(_df)
        
        # # select first 20 rows for _df
        # _df = _df.iloc[:20]
        # print(_df)
            
        _df_sum = _df.drop(columns=['日期', '充值次数']).sum(axis=1)
        _df_value = _df.drop(columns=['日期', '充值次数'])
        # print('_df_value: {}'.format(_df_value))
        # print('_df_sum: {}'.format(_df_sum))
        # for each value in _df_value and _df_sum, calculate its percentag

        # 測試讀取公共opts API
        # c = (
        bar = \
            Bar(init_opts=opts.InitOpts(width="1420px", height="1420px", theme=ThemeType.LIGHT)) \
            .add_xaxis(['充值1次', '充值2次', '充值3次', '充值4次', '充值5次', '充值6次', '充值7次', '充值8次', '充值9次', '充值10次']) \
            # .set_series_opts(
            #     label_opts=opts.LabelOpts(
            #         position="right",
            #         formatter=JsCode(
            #             # "function (x) { console.log(x); return x; }"
            #             "function(x){return Number(x.data.percent * 100).toFixed() + '%';}"\
                    # ),
                # )
            # )
        
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
        ), title_opts=opts.TitleOpts(title="{} Bar-堆叠数据（全部）".format(date)))
        # bar.set_global_opts(title_opts=opts.TitleOpts(title="{} Bar-堆叠数据（全部）".format(date)))
        bar.render("stack_bar_percent.html")

        # 转置
        # _df = _df.T
        # # drop the first line
        # _df = _df[1:]
        # # convert first row as column name
        # _df.columns = _df.iloc[0]
        # _df = _df[1:]
        # # convert index as new column alias vip_level
        # _df.reset_index(inplace=True)
        # _df.rename(columns={'index': 'vip_level'}, inplace=True)
        # # drop the index
        # _df.reset_index(drop=True, inplace=True)
        # # edit vip_level column value, delete '充值人数' string
        # _df['vip_level'] = _df['vip_level'].apply(lambda x: x.replace('充值人数', ''))

        # print(_df)

        # _df.to_excel('df_{}.xlsx'.format(date), index=True)
        # _df.to_excel('df_T_{}.xlsx'.format(date), index=True)

def main_tes_stack_bar():
    df = pd.read_csv(
        'processed_task_1_3.csv',
        header=0,
        encoding='utf-8'
    )
    print(df)
    
    df = df[['日期', '商品名称', '充值人数']].reset_index(drop=True)

    # create df for each 日期 by iteration
    pies = []
    for date in df['日期'].unique():
        df_date = df[df['日期'] == date]
        # sort by 充值人数
        df_date = df_date.sort_values(by='充值人数', ascending=False)
        df_date = df_date[['商品名称', '充值人数']]
        df_draw = [list(z) for z in zip(df_date['商品名称'], df_date['充值人数'])]
        print(df_draw)
        draw(df_draw, title='top20商品消费人数占比(相对)', subtitle=date).render('pie_{}.html'.format(date))

if __name__ == "__main__":
    # main_test()
    main_test_faker_data_structure()




   