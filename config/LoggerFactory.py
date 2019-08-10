import logging, sys
from logging.handlers import RotatingFileHandler

from config import constants


class LoggerFactory():
  rot_handler = None
  all_loggers = []

  def __init__(self):
    self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    self.handler_stream = logging.StreamHandler(sys.stdout)
    self.handler_stream.setFormatter(self.formatter)
    self.handler_stream.setLevel(logging.INFO)

    self.rot_handler = RotatingFileHandler(constants.LOGGING_FILE_PATH, maxBytes=2000000, backupCount=5)
    self.rot_handler.setFormatter(self.formatter)

    self.file_handler = logging.FileHandler(constants.LOGGING_FILE_PATH)

  def create_logger(self, name):
    print(f"Creating logger {name} ...")
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # NOTE: This is problematic when using Spark.
    # logger.addHandler(self.rot_handler)

    logger.addHandler(self.file_handler)

    logger.addHandler(self.handler_stream)

    self.all_loggers.append(logger)

    return logger

