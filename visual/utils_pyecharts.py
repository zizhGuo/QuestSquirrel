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
import functools

    
def plot_pie(func):
    @functools.wraps(func)
    def wrapper(self, df, *args, **kwargs):
        """
        kwargs = {
            'class': str
            'title': str,
            'dt_T': str,
            'columns': List[string],
        }
        
        """
        print('enter plot_pie')
        print('kwargs: ', kwargs)
        print('df')
        print(df)
        assert 'column' in kwargs, "No columns provided"
        
        _df = df[[kwargs['class'], kwargs['column']]]
        print(""" kwargs['total_n_players']""", kwargs['total_n_players'])

        _df['total_n_players'] = kwargs['total_n_players'].astype(str)
        _df['class_with_total_n_players'] = _df[kwargs['class']] + '|' + _df['total_n_players']
        pie_data = list(_df[['class_with_total_n_players', kwargs['column']]].itertuples(index=False, name=None))
        pie = Pie()
        pie.add(
            series_name="",
            data_pair=pie_data,
            radius=["30%", "75%"],
            center=["50%", "50%"],
            rosetype="radius",
        )
        pie.set_global_opts(
            title_opts=opts.TitleOpts(title=f"{kwargs['title']}- 【{kwargs['column']}】 - 日期：{kwargs['dt_T']}"),
            legend_opts=opts.LegendOpts(is_show=False),
        )
        pie.set_series_opts(tooltip_opts=opts.TooltipOpts(formatter=JsCode("""
        function(params) {
            var [vip, n_players] = params.name.split('|');
            return 
                'VIP'+ vip 
                +'  人数：' + params.value + '/'+n_players + '(总)';
        }
        """)))
        pie.set_series_opts(label_opts=opts.LabelOpts(formatter=JsCode("""
        function(params) {
            var [vip, n_players] = params.name.split('|');
            return 'VIP'+ vip + 
                '： ' + params.percent.toFixed(1) + '%';
        }
        """)))

        func(self, *args, **kwargs)
        return pie
    return wrapper
