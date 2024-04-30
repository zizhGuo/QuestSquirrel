from abc import abstractmethod

import os
import sys
print(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'visual')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'visual')))

class VisualizerScheduler:
    """
        represents the vis task engine as a schedular
    """
    def __init__(self, config, root_path, module) -> None:
        # TODO config validator or local validator
        self.config = config
        self.module = module
        self.root_path = root_path
        self.visualizer_generator = []
        for _vis, sub_config in config[module.visual_module].items():
            print(f'_vis: {_vis}')
            _generator = VisualizerGenerator(sub_config, root_path, config['end_dt'])
            self.visualizer_generator.append(_generator)

    def run(self):
        for i in range(len(self.visualizer_generator)):
            print('VisualizerScheduler run()')
            try:
                # print(f'generating visual: {i}')
                self.visualizer_generator[i].run()
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
            except Exception as e:
                print(e)

class Generator:
    def __init__(self) -> None:
        pass
    
    @abstractmethod
    def run(self):
        pass

    def _load_args(self, sub_config):
        def _get_element(config_list, idx, ranged = False):
            return config_list[idx] if not ranged else config_list[idx:]
        if sub_config is not None:
            on = _get_element(sub_config, 0)
            mod = _get_element(sub_config, 1)
            cls = _get_element(sub_config, 2)
            src_tbls = _get_element(sub_config, 3, ranged=True)
        return [on, mod, cls, src_tbls]

class VisualizerGenerator(Generator):
    """
        reprensent a sub vis page that contains multiple chunks
    """
    def __init__(self, sub_config, root_path, end_dt) -> None:
        self.sub_config = sub_config
        self.root_path = root_path
        self.end_dt = end_dt
        # self.output_file = sub_config['output_file']
        self.temp_save_path = sub_config['temp_save_path']
        self.base_dir = os.path.join(self.root_path, self.temp_save_path, self.end_dt)
        self.output_dir = os.path.join(self.root_path, self.temp_save_path, self.end_dt, sub_config['output_file'])
        self.visualizer = sub_config['visualizer']
        # self.regular = sub_config['regular']
        self._load_visualizer()
        
        # TODO 初始化pyecharts page 

    @staticmethod
    def _get_element(config_list, idx, ranged = False):
        return config_list[idx] if not ranged else config_list[idx:]
    
    def _gen_params(self):
        return {'sub_config': self.sub_config, 
                'root_path': self.root_path, 
                'end_dt': self.end_dt,
                'output_dir': self.output_dir,
                'base_dir': self.base_dir
                }
    
    def _load_visualizer(self):
        """
            initialize visualizers instances
        """
        self.visualizers = []
        for _ in self.visualizer:
            _args = self._load_args(_)
            # TODO 将param处理逻辑在 业务类 实现，该模块不负责专门针对template生成params

            # ？待处理
            # _params = self._process_args(_args)
            _args.append(self._gen_params())

            # ？待处理
            _visualizer = Visualizer(_args)
            self.visualizers.append(_visualizer)

    # def _process_args(self, args):
    #     """
    #         处理args，生成script 需要的参数
    #         TODO
    #         - 可以对template做微笑的参数改进
    #         - 也可以在这里增加判定语句，区别处理params
    #     """
    #     # 需要判断是否是script template，如果是，执行
    #     _dir = os.path.join(self.root_path, self.temp_save_path, self.end_dt)
    #     assert len(args[3]) > 0, "No source tables provided"
    #     params = {file.split('.')[0]: os.path.join(_dir, file) for file in args[3]}
    #     params.update({'dt_T': int(self.end_dt)})
    #     return params

    def run(self):
        for visualizer in self.visualizers:
            try:
                # ret represents a page chunk
                ret = visualizer.run()
                print(ret)
                #TODO
                 # add ret to the page to return
                
            except Exception as e:
                print(e)

class Visualizer:
    """
        represent a vis chunk
            store the args
            store args
            dynamically import the module
            create the instance
            run() to return the html chunk
    """
    def __init__(self, args) -> None:
        # (self.on, self.mod, self.cls, self.src_tbls)
        self.args = args
        self.on = args[0]
        self.mod = args[1]
        self.cls = args[2]
        # self.src_tbls = args[3]   
        # self.arg = args[-1]
        # print('args: {}'.format(args))

    def run(self):
        # return ''
        if self.on == 1:
            print("enter visualizer")
            visual = self._create_visual_instance()(self.args)
            # return 'visualizer run()'
            return visual.plot()
        else:
            print('skip visualizer')
            return None
    
    def _create_visual_instance(self):
        import importlib
        print(f'dynamically import module: {self.mod}')
        module = importlib.import_module(self.mod)
        print(f'dynamically create visual class: {self.cls}')
        Class = getattr(module, self.cls)
        print('Class: ', Class)
        return Class