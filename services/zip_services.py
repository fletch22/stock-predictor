import ctypes
from ctypes import windll
import os
import zipfile

from datetime import datetime
from stopwatch import Stopwatch

import config
from config.logger_factory import logger_factory
from services import file_services
from utils import date_utils

logger = logger_factory.create_logger(__name__)

def zipdir(path, ziph, omit_folders: list=None):
  root_len = len(path)
  for root, dirs, files in os.walk(path):
    for f in files:
      source_path = os.path.join(root, f)
      arcname = f"{root[root_len:]}/{f}"
      skip_file = False
      for o in omit_folders:
        if arcname.startswith(o):
          skip_file = True

      if not skip_file:
        print(f"{arcname}")
        ziph.write(source_path, arcname)


def backup_project():
  project_path = os.path.join(config.constants.PROJECT_DIR)
  backup_root = config.constants.BACKUP_ROOT_PATH

  basename = date_utils.format_file_system_friendly_date(datetime.now())
  backup_file_path = os.path.join(backup_root, f"{basename}.zip")

  zipf = zipfile.ZipFile(backup_file_path, 'w', zipfile.ZIP_DEFLATED)

  volume = file_services.get_windows_drive_volume_label(backup_root[0])

  logger.info(f"Volume Name: '{volume}'.")

  if volume != config.constants.BACKUP_VOLUME_LABEL:
    raise Exception("Error. Backup failed! Volume label does not match expected label.")

  stopWatch = Stopwatch()
  stopWatch.start()
  zipdir(project_path, zipf, omit_folders=["\\venv"])
  zipf.close()
  stopWatch.stop()

  logger.info(f"Elapsed seconds: {stopWatch}")



if __name__ == '__main__':
  backup_project()