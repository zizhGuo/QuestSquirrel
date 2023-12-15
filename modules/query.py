class QueryManager:
    def __init__(self, config, root_path):
        assert config is not None, "Query is None"
        try:
            self.template_names = config['template_names']
            self.info = config['info']
            self.parameters = config['params']
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