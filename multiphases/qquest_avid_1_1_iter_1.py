import os
import sys

import pandas as pd

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, when

print('append module path: {}'.format(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from modules_fairyland.decorators import retry
from qquest_base import QuestTaskBase

PARAMS = {
    'Envir3Avid': {
        'app_name': 'Envir3Avid',
        'logger_name': 'Envir3Avid_logger',
        'column_name': ['日期' ,'总游戏人数' ,'类别', '类别人数', '去重类别人数'],
    }
}


class QuestLocal(QuestTaskBase):
    def __init__(self, params, *args, **kwargs) -> None:
        super().__init__(params, *args, **kwargs)

    def _create_or_get_spark(self):
        print('overwrite _create_or_get_spark')
        spark = SparkSession.builder \
            .appName("Local SparkSession") \
            .master("local[*]") \
            .getOrCreate()
        spark.sql("show tables;").show()
        return spark

class Envir3Avid(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)
        # spark = self._create_or_get_spark()

    def query_and_save(self):
        super().query_and_process_v2(super().query_v1, super().process_v1)