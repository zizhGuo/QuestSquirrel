
class Flag:
    def __init__(self, config, **kwargs):
        try:
            self.config = config
            """
                Use for top level control
            """ 
            self.hivequery = config['hivequery']
            self.dataprocess = config['dataprocess']
            self.read_from_file_dataprocess = config['read_from_file_dataprocess']
            self.reportgenerate = config['reportgenerate']
            self.running_tasks = config['running_tasks']
            self.logging = config['logging']
            self.visual = config['visual']
            self.read_from_file_visual = config['read_from_file_visual']
            self.email = config['email']
        except Exception as e:
            print('Flag inside init failed.')
            print(e)
            return
