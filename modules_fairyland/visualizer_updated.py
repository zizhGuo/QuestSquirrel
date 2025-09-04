from abc import abstractmethod
import traceback

import os
import sys
print(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'visual')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'visual')))

from pyecharts.charts import Page

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
                self.visualizer_generator[i].run()
            except Exception as e:
                print('VisualizerScheduler (task level) -> visualizer_generator.run() failed ', e)
                print(traceback.format_exc())
            else:
                try:
                    self.visualizer_generator[i].page_add_and_file_save()
                except Exception as e:
                    print('VisualizerScheduler (task level) -> visualizer_generator.page_add_and_file_save() failed ', e)
                    print(traceback.format_exc())
            finally:
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')


class Generator:
    def __init__(self) -> None:
        pass
    
    @abstractmethod
    def run(self):
        pass

    def _load_args(self, sub_config) -> list:
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
        self.temp_save_path = sub_config['temp_save_path']
        self.title = sub_config['title']
        self.base_dir = os.path.join(self.root_path, self.temp_save_path, self.end_dt)
        self.output_dir = os.path.join(self.root_path, self.temp_save_path, self.end_dt, sub_config['output_file'])
        self.visualizer = sub_config['visualizer']
        self._load_visualizer()
        
        self.page = Page(page_title=self.title)
    
    def _gen_params(self):
        return {
                # 'sub_config': self.sub_config, 
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

            # 将全部参数传入，业务类按需要取用
            _args.append(self._gen_params())

            # ？待处理
            _visualizer = Visualizer(_args)
            self.visualizers.append(_visualizer)

    def run(self):
        self.chunks = []
        for visualizer in self.visualizers:
            try:
                # ret represents a page chunk
                ret = visualizer.run()
                print('结果：', ret) # None or html chunk
                #TODO
                 # add ret to the page to return
            except TypeError as e:
                print('VisualizerGenerator (page level) -> visualizers.run() failed ,ret not a list', e)
            except Exception as e:
                print('VisualizerGenerator (page level) -> visualizers.run() failed ,', e)
                print(traceback.format_exc())
            else:
                self.chunks.extend(ret)
            finally:
                print('-----------------------------------')
                print('-----------------------------------')
                print('-----------------------------------')
        
    def page_add_and_file_save(self):
        [self.page.add(chunk) for chunk in self.chunks]
        self.page.render(self.output_dir)

class Visualizer:
    """
        represent a vis chunk
            store the args
            store args
            dynamically import the module
            create the instance
            run() to return the html chunk
        a template represents a page
    """
    def __init__(self, args) -> None:
        # (self.on, self.mod, self.cls, self.src_tbls)
        self.args = args
        self.on = args[0]
        self.mod = args[1]
        self.cls = args[2]
        print('args: ', args)
        # self.src_tbls = args[3]   
        # self.arg = args[-1]
        # print('args: {}'.format(args))

    def run(self):
        # return ''
        if self.on == 1:
            print("enter visualizer")
            # print('self.args, ', self.args)
            # 创建业务类实例
            try:
                visual = self._create_visual_instance()(self.args)
                print('create visual instance success.')
            except KeyError as e:
                print('Visualizer -> create visual instance failed, KeyError: ', e)
                return 401
            except ImportError as e:
                print(f"ImportError: {e}")
                return 402
            except Exception as e:
                print('Visualizer -> create visual instance failed, Exception: ', e)
                return 400
            # return 'visualizer run()'
            else:
                try:
                    return visual.plot()
                except e as Exception:
                    print('visualizer (chunk level)-> visual.plot() failed: ', e)
            finally:
                print('-----------------------------------')
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