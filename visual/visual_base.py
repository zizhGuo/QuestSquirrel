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

    
    