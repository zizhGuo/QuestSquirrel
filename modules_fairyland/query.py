class QueryManager:
    def __init__(self, config, root_path, param_dt, module, sub_task):
        assert config is not None, "Config is None"
        print('enter QueryManager init.')
        try:
            
            self.template_names = config[module.query_module][sub_task]['template_names']
            self.info = config[module.query_module][sub_task]['info']
            self.params = config[module.query_module][sub_task]['params']
            # self.parameters = config['params']
            self.parameters = {**param_dt, **self.params}
            self.root_path = root_path
            
            print(self.template_names)
            print(self.info)
            print(self.parameters)


            # load queries
            self.query_runs = {}
            for i in range(len(self.template_names)):
                template = self._load_template(self.root_path, self.template_names[i])
                query_run = self._process_query(self._fill_template(template))
                self.query_runs[self.template_names[i]] = query_run
        
        except Exception as e:
            print('Query INFO Load failed.')
            print(e)
            return
    
    def print_query_runs(self):
        print(self.query_runs)

    def _load_template(self, path, template_name):
    # Assuming the templates are in 'query_templates/' relative to the main.py file
    # assert the dir has the query_templates folder
        req_cat = self.info['req_cat']
        req_iter = self.info['req_iter']
        req_status = self.info['req_status']

        print(f'{path}/templates/{req_cat}/{req_iter}/{req_status}/{template_name}.sql')
        with open(f'{path}/templates/{req_cat}/{req_iter}/{req_status}/{template_name}.sql', 'r', encoding='UTF-8') as file:
            return file.read()
    
    def _fill_template(self, template):
        return template.format(**self.parameters)
    
    def _commands2subcmd(self, cmd):
        import re
        cmd = re.sub(r'\\`', r'\`', cmd)
        cmd = re.sub(r'\'', '\'', cmd)
        return cmd

    def _process_query(self, query):
        import re
        query = query.replace('\n', ' ')  # Remove newline characters
        query = re.sub(r'\s+', ' ', query)  # Replace multiple whitespaces with a single whitespace
        # query = re.sub('`', r'\`', query)
        return query
    
    def test_get_query(self):
        return self.query_runs

class QueryManager_manual:
    def __init__(self):
        print('creating QueryManager_manual')
    
    def get_sql_run(self, sql:str):
        sql_run = self._process_query(sql)
        return sql_run

    def load_all_query_from_dir(self, directory):
        # iterate all sql files in 'path' directory
        import os
        sql_file_contents = []
        # sort os.listdir(directory)
        # os.listdir(directory).sort()

        for file in sorted(os.listdir(directory)):
            # Construct the full path of the file
            print(file)
            if file == 'all.sql':
                continue
            full_path = os.path.join(directory, file)
            # Check if it's a file and ends with .sql
            if os.path.isfile(full_path) and file.endswith('.sql'):
                with open(full_path, 'r', encoding='utf-8') as sql_file:
                    content = sql_file.read()
                    sql_file_contents.append(self._process_query(content))
        return sql_file_contents

    def _process_query(self, query):
        import re
        query = query.replace('\n', ' ')  # Remove newline characters
        query = re.sub(r'\s+', ' ', query)  # Replace multiple whitespaces with a single whitespace
        # query = re.sub('`', r'\`', query)
        return query