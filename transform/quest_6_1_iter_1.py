



class T_quest_6_1_iter_1_envir_4:
    def __init__(self, df) -> None:
        print('import T_quest_6_1_iter_1_envir_4 success')
        self.df = df
        # assert config is not None, "Config is None"
        # assert config['module'] is not None, "module is None"
        # self.connector_module = config['module']['connector_module']
        # self.query_module = config['module']['query_module']
        # self.report_module = config['module']['report_module']
        # self.visual_module = config['module']['visual_module']
        # self.email_module = config['module']['email_module']
    def edit(self):
        # drop column '渔场id'
        self.df = self.df.drop(columns=['渔场id'])
        return self.df
    
    def show(self):
        print('show T_quest_6_1_iter_1_envir_4')
        print(self.df.columns)
        print(self.df.head(10))