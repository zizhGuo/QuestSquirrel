import os
import sys
CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_FILE_DIR)
pp_dir = os.path.dirname(PARENT_DIR)
sys.path.insert(0, CURRENT_FILE_DIR)
sys.path.insert(1, PARENT_DIR)
print(f'parent dir: {pp_dir}')
from modules_fairyland.arguments import parser
from modules_fairyland.config import ConfigManager
from modules_fairyland.schedular_fairydemon import TaskScheduler
from modules_fairyland.date_converter import date2str, dt_minus_days
from modules_fairyland.report_fairydemon import ReportSchedular
# from modules_fairyland.visualizer_fairydemon import Visualizer
from modules_fairyland.email import EmailScheduler
from modules_fairyland.visualizer_updated import VisualizerScheduler


from datetime import datetime
now = datetime.now()

class Module:
    def __init__(self, config) -> None:
        assert config is not None, "Config is None"
        assert config['module'] is not None, "module is None"
        self.connector_module = config['module']['connector_module']
        self.query_module = config['module']['query_module']
        self.report_module = config['module']['report_module']
        self.visual_module = config['module']['visual_module']
        self.email_module = config['module']['email_module']

def main():
    print("entered main fairydemon land")

    # define sql params: date in dt format "yyyyMMdd"
    args = parser.parse_args()

    if args.config_file:
        CONFIG_FILE = args.config_file
        print(f'Using config file: {CONFIG_FILE}')
    else:
        print('Can\'t find config file. ')
        return
    
    # load config file
    config_path = os.path.join(CURRENT_FILE_DIR, CONFIG_FILE)
    config_manager = ConfigManager(config_path)
    config = config_manager.config


    # end_dt = date2str(datetime.strptime(args.end_dt, "%Y-%m-%d"))
    
    if config['test']['launch']:
        end_dt = dt_minus_days(config['test']['params']['end_dt'], 1)
        start_dt = dt_minus_days(config['test']['params']['end_dt'], config['test']['params']['gap_days'])
    else:
        end_dt = dt_minus_days(args.end_dt, 1)
        start_dt = dt_minus_days(args.end_dt, 2)

    param_dt = {
        'end_dt': end_dt
        ,'start_dt':start_dt
    }
    config['end_dt'] = param_dt['end_dt']
    config['start_dt'] = param_dt['start_dt']

    module = Module(config)
    if config['steps']['query']:
        scheduler = TaskScheduler(config = config, root_path = CURRENT_FILE_DIR, param_dt = param_dt, module = module)
        scheduler.run_tasks()

    if config['steps']['report']:
        reporter = ReportSchedular(config = config, root_path = CURRENT_FILE_DIR, module = module)
        reporter.run()
    
    if config['steps']['visual']:
        # visulizer = Visualizer(config = config, root_path = CURRENT_FILE_DIR, module = module)
        visulizer = VisualizerScheduler(config = config, root_path = CURRENT_FILE_DIR, module = module)
        visulizer.run()

    if config['steps']['email']:
        emailer = EmailScheduler(config = config, root_path = CURRENT_FILE_DIR, module = module)
        emailer.send_email()

if __name__ == "__main__":
    main()