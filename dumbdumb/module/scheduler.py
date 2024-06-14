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
    def parse_jobs(config_file):
        config_yaml = ConfigLoader.load_config(config_file)
        jobs = config_yaml['jobs']
        connection = config_yaml['connection']

        for job in jobs:
            try:
                flag = job[0]
                mode_dict = job[1]
                sql_dict = job[2]
            
                if flag == 0:
                    continue
                config_dict = {}
                config_dict.update(mode_dict)
                config_dict.update(sql_dict)

                if not 'end_date' in sql_dict:
                    sql_dict.update(ConfigLoader.get_today_date())
                config_dict.update(ConfigLoader._get_sql(sql_dict))

                if config_dict['dbtype'] == 'ch':
                    config_dict.update(connection['ch'])
                elif config_dict['dbtype'] == 'mysql_statistics':
                    config_dict.update(connection['mysql_statistics'])
                elif config_dict['dbtype'] == 'mysql_log':
                    config_dict.update(connection['mysql_log'])
                else:
                    raise ValueError('dbtype not found')
            except Exception as e:
                print(f'Error: {e}')
            else:
                print('job config set')
                print(f'config_dict: {config_dict}')
                yield config_dict


class Schedular:
    def __init__(self, config_file):
        self.config_file = config_file

    def run(self):
        for config_dict in ConfigLoader.parse_jobs(self.config_file):
            # print(config_dict)
            # pass
            ParquetWriter.start_pipeline(config_dict)

if __name__ == '__main__':
    args = parser.parse_args()
    config_file = os.path.join(PARENT_DIR, 'config', args.config_file)

    schedular = Schedular(config_file)
    schedular.run()