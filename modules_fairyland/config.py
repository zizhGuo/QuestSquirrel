import yaml


class ConfigManager:
    def __init__(self, config_path):
        self.config = self.load_config(config_path)

    def load_config(self, config_path):
        # 加载配置文件
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        return config

    def get_task_config(self, task_name):
        # 返回特定任务的配置
        pass

    def test_get_config(self):
        return self.config

