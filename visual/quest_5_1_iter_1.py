"""
此模块用于实现任务5_1的第一次迭代的商城图示
"""
import os

from abc import abstractmethod

import logging


class BaseVisual:
    def __init__(self):
        self.name = 'Base'
    
    @abstractmethod
    def plot(self):
        pass

# class ScriptVisualizer(BaseVisual):
#     def __init__(self, params):
#         super().__init__(params)
#         self.name = 'Script'

#     @abstractmethod
#     def load_template(self):
#         print('load_template')
    
# class RegularVisualizer(BaseVisual):
#     def __init__(self, params):
#         super().__init__(params)
#         self.name = 'Regular'

#     @abstractmethod
#     def load_data(self):
#         print('load_data')
    
#     @abstractmethod
#     def plot(self):
#         print('plot')
    
#     def run(self):
#         self.load_data()
#         self.plot()

class FairydemonMall(BaseVisual):
    def __init__(self, args):
        print('entered FairydemonMall initialized')
        super().__init__()
        self.name = 'Test'
        self.args = args
        self.params = self._gen_params(args)
        print(f'params: {self.params}')

    def _gen_params(self, args):
        _dir = os.path.join(args[-1]['root_path'], args[-1]['temp_save_path'], args[-1]['end_dt'])
        assert len(args[3]) > 0, "No source tables provided"
        print('entered _gen_params')
        params = {file.split('.')[0]: os.path.join(_dir, file) for file in args[3]}
        params.update({'dt_T': int(args[-1]['end_dt'])})
        return params
        
    def plot_v1(self):
        print('plot_v1')
    
    def plot(self):
        # super().plot()
        self.plot_v1()
        return 'test'

class Test2(BaseVisual):
    def __init__(self, args):
        super().__init__()
        self.name = 'Test2'
        self.args = args

    # def load_template(self):
    #     print('load_template')
    
    def plot_v2(self):
        print('plot_v2')
    
    def plot(self):
        # super().plot()
        # self.load_template()
        self.plot_v2()
        return 'test2'




yaml_str = """
visual:
  vis1:
    output_file: 'quest_fairydemon_visual.html'
    temp_save_path: "data/quest_fairydemon"
    scripts:
        - [0, 1, 'quest_5_1_iter_1', 'Test', 'dws_mall_task1.xlsx.xlsx', 'dws_mall_task2.xlsx.xlsx', 'dws_mall_task1.xlsx.xlsx', 'dws_mall_task1.xlsx.xlsx']
        - [1, 1, 'quest_5_1_iter_1', 'Test2', 'dws_mall_task1.xlsx.xlsx']
    regular:
      [1, 1, 'quest_5_1_iter_1', 'Test2', 'dws_mall_task1.xlsx']
"""

import yaml

# using yaml package to load the yaml string
config = yaml.load(yaml_str, Loader=yaml.FullLoader)
# logging.info(f'config: {config}')
# print(f'config: {config}')
# print(config.get('visual').get('vis1').get('scripts'))


# for item in config.get('visual').get('vis1').get('scripts'):
#     print(item)

# t2 = Test2(params=None)
# t2.plot()