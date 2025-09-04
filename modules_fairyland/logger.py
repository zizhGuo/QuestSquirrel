import logging

class Logger:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def log(self, message):
        self.logger.info(message)

    def log_error(self, message):
        self.logger.error(message)

class NullLogger:
    def debug(self, *args, **kwargs):
        pass
    def info(self, *args, **kwargs):
        pass
    def warning(self, *args, **kwargs):
        pass
    def error(self, *args, **kwargs):
        pass
    def critical(self, *args, **kwargs):
        pass

class BaseLogger:
    def __init__(self, DEBUG):
        self._set_logger()
        if DEBUG:
            self.logger = NullLogger()

    def _set_logger(self):
        # Use the class name to initialize the logger's name
        class_name = self.__class__.__name__
        self.logger = logging.getLogger(class_name)
        self.logger.setLevel(logging.DEBUG)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(console_handler)