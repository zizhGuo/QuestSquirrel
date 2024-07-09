import traceback

from pyspark.sql import SparkSession

from multiphases.qquest_local import QuestLocalUpdate


PARAMS = {
    'UpdateDWDWaveTrigger': {
        'app_name': 'UpdateDWDWaveTrigger',
        'logger_name': 'UpdateDWDWaveTrigger_logger',
    }
    # TODO UpdateDWDLogOrderWaveTrigger
    ,'UpdateDWDLogOrderWaveTrigger': {
        'app_name': 'UpdateDWDLogOrderWaveTrigger',
        'logger_name': 'UpdateDWDLogOrderWaveTrigger_logger',
    }
}

class UpdateDWDWaveTrigger(QuestLocalUpdate):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)

    @staticmethod
    def print_attributes(instance):
        for attribute, value in instance.__dict__.items():
            print(f"{attribute}: {value}")

    def query_and_save(self):
        # UpdateDWDWaveTrigger.print_attributes(self)
        # super().query_and_update(super().query_v2, super().save_data_parquet)
        super().query_and_save()

class UpdateDWDLogOrderWaveTrigger(QuestLocalUpdate):
    def __init__(self, params, *args, **kwargs) -> None:
        kwargs.update({'PARAMS': PARAMS})
        super().__init__(params, *args, **kwargs)
    
    def query_and_save(self):
        # super().query_and_update(super().query_v2, super().save_data_parquet)
        super().query_and_save()