import os
import sys
CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_FILE_DIR)
sys.path.insert(0, CURRENT_FILE_DIR)
sys.path.insert(1, PARENT_DIR)

from modules_fairyland.arguments import parser
from modules_fairyland.config import ConfigManager
from modules_fairyland.schedular_fairydemon import TaskScheduler
from modules_fairyland.date_converter import date2str, dt_minus_days
from modules_fairyland.report_fairydemon import ReportGenerator
from modules_fairyland.visualizer_fairydemon import Visualizer
from modules_fairyland.email import Email

CONFIG_FILE = 'config_fairydemon.yaml'

from datetime import datetime
now = datetime.now()

def main():
    print("entered main fairydemon land")

    # load config file
    config_path = os.path.join(CURRENT_FILE_DIR, CONFIG_FILE)
    config_manager = ConfigManager(config_path)
    config = config_manager.config

    # define sql params: date in dt format "yyyyMMdd"
    args = parser.parse_args()
    # end_dt = date2str(datetime.strptime(args.end_dt, "%Y-%m-%d"))
    end_dt = dt_minus_days(args.end_dt, 1)
    start_dt = dt_minus_days(args.end_dt, 2)

    param_dt = {
        'end_dt': end_dt
        ,'start_dt':start_dt
    }
    config['end_dt'] = param_dt['end_dt']
    config['start_dt'] = param_dt['start_dt']

    scheduler = TaskScheduler(config = config, root_path = CURRENT_FILE_DIR, param_dt = param_dt)
    scheduler.run_querys()

    reporter = ReportGenerator(config = config, root_path = CURRENT_FILE_DIR)
    reporter.run()

    visulizer = Visualizer(config = config, root_path = CURRENT_FILE_DIR)
    visulizer.run()

    emailer = Email(config = config, root_path = CURRENT_FILE_DIR)
    emailer.send_email()

if __name__ == "__main__":
    main()