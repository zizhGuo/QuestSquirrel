import os
CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_FILE_DIR)

from writer import ParquetWriter
# from config_loader import ConfigLoader
from arguments import parser
from tool import process_query, fill_template

import yaml


class ConfigLoader:
    def __init__(self, config_file) -> None:
        self.config_yaml = ConfigLoader.load_config(config_file)
        jobs = self.gen_jobs_config()
        connection = self.gen_coonection_config()
    
    @staticmethod
    def load_config(config_path):
            with open(config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
            return config
    
    def _gen_jobs_config(self):
        return self.config_yaml['jobs']

    def _gen_coonection_config(self):
        return self.config_yaml['connection'] 

    @staticmethod
    def get_today_date():
        import datetime
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        # yesterday date
        start_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        return {'end_date': end_date, 'start_date': start_date}

    @staticmethod
    def _get_sql(config):
        sql_file = os.path.join(PARENT_DIR, 'sql', config['file'])
        with open(sql_file) as file:
            sql = file.read()
        sql = process_query(sql)
        sql = fill_template(sql, config)
        return {'sql': sql}
    
    @staticmethod
    def _gen_config_dict(mode_dict, sql_dict, connection, tail=0):
        config_dict = {}
        config_dict.update(mode_dict)

        if tail:
            _tb=sql_dict['table']
            _start_dt = sql_dict['start_date'].replace("-", "")
            sql_dict.update({'table': _tb + f'_{_start_dt}'})

        
        config_dict.update(ConfigLoader._get_sql(sql_dict))
        config_dict.update(sql_dict)

        if config_dict['dbtype'] == 'ch':
            config_dict.update(connection['ch'])
        elif config_dict['dbtype'] == 'mysql_statistics':
            config_dict.update(connection['mysql_statistics'])
        elif config_dict['dbtype'] == 'mysql_log':
            config_dict.update(connection['mysql_log'])
        else:
            raise ValueError('dbtype not found')
        return config_dict
    
    @staticmethod
    def _gen_datelist(end_date, start_date):
        import datetime
        # end_date.replace('-', '')
        start_dt = start_date.replace('-', '')
        end_dt = end_date.replace('-', '')
        # date_list = [(start_date, start_date_plus1, start_dt)]
        date_list = []
        # convert start_date to string Ymd
        while start_date != end_date:
            start_date_plus1 = (datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
            start_dt = start_date.replace('-', '')
            date_list.append((start_date, start_date_plus1, start_dt))
            start_date = start_date_plus1
            # end_date = (datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
            # end_dt = end_date.replace('-', '')
            # end_date_minus1 = (datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        return date_list
    
    @staticmethod
    def parse_jobs(config_file):
        config_yaml = ConfigLoader.load_config(config_file)
        jobs = config_yaml['jobs']
        connection = config_yaml['connection']

        for job in jobs:
            try:
                flag = job[0]
                mode_dict = job[1]
                sql_dict = job[2]
                config_dicts = []
                config_dict = None

                if flag == 0:
                    continue
                if mode_dict.get('mode') == 'daily':
                    """
                    日更模式
                    默认部署当天的数据, 强行覆盖当日和昨日日期
                    """
                    sql_dict.update(ConfigLoader.get_today_date())

                    if sql_dict.get('tail_table', 0) == 0:
                        # 非tail表
                        config_dict = ConfigLoader._gen_config_dict(mode_dict, sql_dict, connection)
                    else:
                        config_dict = ConfigLoader._gen_config_dict(mode_dict, sql_dict, connection, tail = 1)
                
                if mode_dict.get('mode') == 'regular':
                    """
                    常规模式
                    """
                    # assert 'end_date' in sql_dict, 'end_date not found'
                    # assert 'start_date' in sql_dict, 'start_date not found'

                    if sql_dict.get('tail_table', 0) == 0:
                        # 非tail表
                        config_dict = ConfigLoader._gen_config_dict(mode_dict, sql_dict, connection)
                    else:
                        config_dict = ConfigLoader._gen_config_dict(mode_dict, sql_dict, connection, tail = 1)

                if mode_dict.get('mode') == 'ranged':
                    """
                    loop范围模式, assert end_date in sql_dict
                    """
                    assert 'end_date' in sql_dict, 'end_date not found'
                    assert 'start_date' in sql_dict, 'start_date not found'

                    end_date = sql_dict['end_date']
                    start_date = sql_dict['start_date']
                    import copy
                    date_list = ConfigLoader._gen_datelist(end_date, start_date)
                    
                    for date in date_list:
                        _sql_dict = copy.deepcopy(sql_dict)
                        _sql_dict.update({'start_date': date[0], 'end_date': date[1]})
                        _config_dict = ConfigLoader._gen_config_dict(mode_dict, _sql_dict, connection,
                                                                     tail = sql_dict.get('tail_table', 0))
                        config_dicts.append(_config_dict)

                if mode_dict.get('add_columns'):
                    """
                    添加列模式
                    """
                    mode_dict['add_columns'] \
                        .update({mode_dict['partition_cols']:
                                  ConfigLoader.get_today_date()['start_date'].replace('-', '')})

                # if sql_dict.get('tail_table') == 1:
                #     print('tail_table found')
                #     assert 'end_date' in sql_dict, 'end_date not found'
                #     # given end_date in format of 'yyyy-MM-dd', and start_date in format of 'yyyy-MM-dd'
                #     # generate for loop for each date in the range
                #     end_date = sql_dict['end_date']
                #     start_date = sql_dict['start_date']
                #     date_list = ConfigLoader._gen_datelist(end_date, start_date)
                #     _tb =sql_dict['table']
                #     for date in date_list:
                #         # sql_dict['end_date'] = date
                #         sql_dict.update({'table': _tb+f'_{date[2]}', 'start_date': date[0], 'end_date': date[1]})
                #         _config_dict = ConfigLoader._gen_config_dict(mode_dict, sql_dict, connection)
                #         config_dicts.append(_config_dict)

            except Exception as e:
                print(f'Error: {e}')
            else:
                print('job config set')
                # print(f'config_dict: {config_dict}')
                if config_dict:
                    yield config_dict
                if config_dicts:
                    for config_dict in config_dicts:
                        yield config_dict

    # @staticmethod
    # def parse_jobs(config_file):
    #     config_yaml = ConfigLoader.load_config(config_file)
    #     jobs = config_yaml['jobs']
    #     connection = config_yaml['connection']

    #     for job in jobs:
    #         try:
    #             flag = job[0]
    #             mode_dict = job[1]
    #             sql_dict = job[2]
    #             config_dicts = []
    #             config_dict = None

    #             if flag == 0:
    #                 continue
    #             if sql_dict.get('tail_table') == 0:
    #                 config_dict = ConfigLoader._gen_config_dict(mode_dict, sql_dict, connection)
    #             if sql_dict.get('tail_table') == 1:
    #                 print('tail_table found')
    #                 assert 'end_date' in sql_dict, 'end_date not found'
    #                 # given end_date in format of 'yyyy-MM-dd', and start_date in format of 'yyyy-MM-dd'
    #                 # generate for loop for each date in the range
    #                 end_date = sql_dict['end_date']
    #                 start_date = sql_dict['start_date']
    #                 date_list = ConfigLoader._gen_datelist(end_date, start_date)
    #                 _tb =sql_dict['table']
    #                 for date in date_list:
    #                     # sql_dict['end_date'] = date
    #                     sql_dict.update({'table': _tb+f'_{date[2]}', 'start_date': date[0], 'end_date': date[1]})
    #                     _config_dict = ConfigLoader._gen_config_dict(mode_dict, sql_dict, connection)
    #                     config_dicts.append(_config_dict)

    #         except Exception as e:
    #             print(f'Error: {e}')
    #         else:
    #             print('job config set')
    #             print(f'config_dict: {config_dict}')
    #             if config_dict:
    #                 yield config_dict
    #             if config_dicts:
    #                 for config_dict in config_dicts:
    #                     yield config_dict


class Schedular:
    def __init__(self, config_file):
        self.config_file = config_file

    def run(self):
        for config_dict in ConfigLoader.parse_jobs(self.config_file):
            # print(config_dict)
            print(config_dict)
            ParquetWriter.start_pipeline(config_dict)
if __name__ == '__main__':
    args = parser.parse_args()
    config_file = os.path.join(PARENT_DIR, 'config', args.config_file)

    schedular = Schedular(config_file)
    schedular.run()