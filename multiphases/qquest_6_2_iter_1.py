import os
import sys

import pandas as pd

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, when

print('append module path: {}'.format(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from modules_fairyland.decorators import retry


class EnvirChurnFisheryActivePacific:
    def __init__(self, params): # TODO params
        self.args = {
            'app_name': 'Quest_6_2_EnvirChurnFisheryActivePacific',
            'start_dt': params['start_dt'],
            'end_dt': params['end_dt'],
            'save_file': params['save_file'],
            'base_dir': params['base_dir'],
            'others': params['others'],
            'column_name': ['日期', '渔场id', '渔场名称', '活跃用户未在对应渔场中游戏用户数']
        }
        self._get_spark()
        self._set_logger()
        print('EnvirChurnFisheryActivePacific init done')

    def _set_logger(self):
        self.logger = logging.getLogger('EnvirChurnFisheryActivePacific_logger')
        self.logger.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
    
    def _get_spark(self):
            # .config("spark.executor.instances", "2") \
            # .config("spark.executor.cores", "2") \
            # .config("spark.yarn.am.priority", "1") \
        
        self.spark = SparkSession.builder \
            .config("spark.yarn.am.priority", "2") \
            .appName(self.args['app_name']) \
            .enableHiveSupport() \
            .getOrCreate()

    def get_config(self):
        config_query = "SELECT gameid, fishery_name, no FROM guozizhun.config_fishery_3d WHERE fisheryname = 'pacific'"
        config_df = self.spark.sql(config_query)
        self.logger.debug('GET config query done.')
        config_df.show()
        return config_df

    # TODO add retry
    def get_cte_data(self):
        cte_query = f"""
        WITH A AS (
            SELECT t1.uid, t1.gameid, t2.fishery_name, t2.no
            FROM b3_statistics.ods_log_gameonline t1
            INNER JOIN (SELECT gameid, fishery_name, no FROM guozizhun.config_fishery_3d WHERE fisheryname = 'pacific') t2
            ON t1.gameid = t2.gameid
            LEFT JOIN b3_statistics.ods_log_gameonline t3
            ON t1.uid = t3.uid AND t1.gameid = t3.gameid AND t3.dt = '{self.args['end_dt']}'
            WHERE t1.dt = '{self.args['start_dt']}' AND t3.uid IS NULL
        )
        , B as (
            select '{self.args['end_dt']}' as dt, A.* from A
        )
        SELECT * FROM B
        """
        A_df = self.spark.sql(cte_query)
        return A_df

    def create_df_column_name(self, config_df):
        fishery_colum = [row.fishery_name for row in config_df.collect()]
        column_name = ['日期', '渔场id', '渔场名称', '活跃用户未在对应渔场中游戏用户数'] + fishery_colum
        print(column_name)
        if self.args.get('column_name', None):
            column_name = self.args['column_name'] + fishery_colum
        return column_name

    def create_aggregations(self, config_df):
        gameid_list = [row.gameid for row in config_df.collect()]

        aggregations = [countDistinct("A.uid").alias("total_distinct_uid")]
        for gameid in gameid_list:
            aggregations.append(countDistinct(when(col("t.gameid") == gameid, col("t.uid"))).alias(f"n_players_{gameid}"))
        return aggregations

    def run_query(self, A_df, aggregations):
        result = A_df.alias("A") \
            .join(self.spark.table("b3_statistics.ods_log_gameonline").alias("t"), 
                  (col("A.uid") == col("t.uid")) & (col("t.dt") == self.args['end_dt']), "left") \
            .groupBy("A.dt", "A.gameid", "A.fishery_name", "A.no") \
            .agg(*aggregations) \
            .orderBy("A.no")
        result = result.drop("no")
        return result
    
    def save_data(self, df):
        """
            add result save function
        """
        format = 'xlsx'
        if self.args['save_file'].split('.')[1] == 'csv':
            format = 'csv'
        # self.result save
        os.makedirs(self.args['base_dir'], exist_ok=True)
        if format == 'csv':
            df.to_csv(self.args['save_file'], index=False)
        else:
            df.to_excel(self.args['save_file'], index=False, engine='openpyxl')

    @retry(max_retries=3, retry_delay=5, exception_to_check=Exception)
    def query_v1(self):
        self.logger.debug(f'started query_v1')
        config_df = self.get_config()
        A_df = self.get_cte_data()
        aggregations = self.create_aggregations(config_df)
        result = self.run_query(A_df, aggregations)
        result.show()
        return config_df, result

    def process_v1(self, config_df, result):
        df = result.toPandas()
        df.columns = self.create_df_column_name(config_df)
        return df

    def query_and_process_v1(self):
        config_df, result = self.query_v1()
        df = self.process_v1(config_df, result)

        return df
    
    @retry(max_retries=3, retry_delay=5, exception_to_check=Exception)
    def query_v2_test(self):
        self.logger.debug('DEBUG FOR TEST: Connector: query_and_save entered.')
        import time
        time.sleep(10)
        # create a dataframe example
        data = {'col1': [1, 2], 'col2': [3, 4]}
        df = pd.DataFrame(data)
        self.logger.debug('DEBUG FOR TEST: Connector: query_and_save sleep done.')
        return df


    def query_and_save(self):
        try:
            result = self.query_and_process_v1()
            # result = self.query_v2_test()
        except Exception as e:
            print('query failed')
            print(e)
            return 'failed'
        else:
            # create
            self.save_data(result)
        finally:
            self.spark.stop()
            print('EnvirChurnFisheryActivePacific done.')