import os
import shutil

import traceback

from pyspark.sql import SparkSession

from qquest_base import QuestTaskBase

class QuestLocal(QuestTaskBase):
    def __init__(self, params, *args, **kwargs) -> None:
        super().__init__(params, *args, **kwargs)

    def _set_args(self):
        if self.params.get('others'):
            try:
                extra_params = self.params['others'][1]
            except Exception as e:
                print(f"Get extra params from others error: {e}")
            else:
                if extra_params.get('end_dt'):
                    if extra_params.get('debug_mode') and extra_params['debug_mode'] == 1:
                        # overwrite save_file for debug
                        print(f'before modified save file: {self.params.get("save_file")}')
                        self.params['save_file'] = \
                            self.params['save_file'].split('.')[0] \
                            + f"_{extra_params['end_dt']}." \
                            + self.params['save_file'].split('.')[1]
                        print(f'after modified save file: {self.params.get("save_file")}')
        super()._set_args()

    def _create_or_get_spark(self):
        print('overwrite _create_or_get_spark')
        spark = SparkSession.builder \
            .appName("Local SparkSession") \
            .master("local[*]") \
            .config("spark.driver.memory", "8g") \
            .config("spark.executor.memory", "8g") \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
        spark.sql("show tables;").show()
        return spark

    def query_and_save(self):
        super().query_and_process_v2(super().query_v1, super().process_v1)

class QuestLocalUpdate(QuestLocal):
    def __init__(self, params, *args, **kwargs) -> None:
        super().__init__(params, *args, **kwargs)
    
    def save_data_parquet(self, result, *args, **kwargs):
        assert self.args.get('parquet_db'), 'parquet db not set'
        assert self.args.get('update') == 1, 'update not set'
        file = os.path.join(self.args['parquet_db'], self.args['template'])
        if self.args.get('partition'):
            part = self.args['partition']

            partitions_to_overwrite = result.select(part).distinct().collect()
            partitions_to_overwrite = [row[part] for row in partitions_to_overwrite]
            for partition in partitions_to_overwrite:
                partition_path = os.path.join(file, f"{part}={partition}")
                if os.path.exists(partition_path):
                    shutil.rmtree(partition_path)

            result.write.mode("append").partitionBy(part).parquet(file)
        else:
            result.write.mode("overwrite").parquet(file)

        print('function: save_data_parquet: create temp view')
        _df = self.spark.read.parquet(file)
        _df.createOrReplaceTempView(file.split("/")[-1])

    def query_and_save(self):
        super().query_and_update(super().query_v2, self.save_data_parquet)
