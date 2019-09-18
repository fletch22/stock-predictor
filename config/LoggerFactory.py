import logging
import os
import sys
from logging.handlers import RotatingFileHandler

from config import constants
from services import file_services


class LoggerFactory():
  rot_handler = None
  all_loggers = []

  def __init__(self):
    self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    self.handler_stream = logging.StreamHandler(sys.stdout)
    self.handler_stream.setFormatter(self.formatter)
    self.handler_stream.setLevel(logging.INFO)

    self.rot_handler = RotatingFileHandler(constants.LOGGING_FILE_PATH, maxBytes=200000, backupCount=5)
    self.rot_handler.setFormatter(self.formatter)

    self.file_handler = logging.FileHandler(constants.LOGGING_FILE_PATH)

  def create_logger(self, name: str, task_dir: str = None):
    print(f"Creating logger {name}; config.constants.IS_IN_SPARK_CONTEXT: {constants.IS_IN_SPARK_CONTEXT}")

    logger = logging.getLogger(name)
    logger.addHandler(self.handler_stream)

    logger.addHandler(self.file_handler)
    # if task_dir is None:
    #   logger.addHandler(self.file_handler)
    # else:
    #   if constants.SPARK_LOGGING_PATH is None:
    #     log_parent_path = os.path.join(task_dir, "logs")
    #   else:
    #     log_parent_path = os.path.join(constants.SPARK_LOGGING_PATH, "logs")
    #
    #   os.makedirs(log_parent_path, exist_ok=True)
    #   log_path = file_services.create_unique_file_system_name(log_parent_path, "sp", extension=".log")
    #
    #   rot_handler = RotatingFileHandler(log_path, maxBytes=2000000, backupCount=5)
    #   rot_handler.setFormatter(self.formatter)
    #
    #   logger.addHandler(self.rot_handler)

    logger.setLevel(logging.INFO)
    self.all_loggers.append(logger)

    return logger

