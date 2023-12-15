import pandas as pd

def draw(df, title, subtitle):
    from pyecharts.charts import Pie
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

if __name__ == "__main__":
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



   