import logging

class Logger:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def log(self, message):
        self.logger.info(message)

    def log_error(self, message):
        self.logger.error(message)
