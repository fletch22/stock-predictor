from datetime import datetime
from os import walk as walker
import os
from zipfile import ZipFile

from utils import date_utils


def walk(dir):
  file_paths = []
  for (dirpath, dirnames, filenames) in walker(dir):
    for f in filenames:
      file_paths.append(os.path.join(dirpath, f))

  return file_paths

def get_filename_info(filepath):
  components = os.path.basename(filepath).split("_")

  category = components[0]
  symbol = components[1]
  datestr = components[2].split(".")[0]

  return {
    "category": category,
    "symbol": symbol,
    "date": date_utils.parse_datestring(datestr)
  }

def create_unique_folder(parent_dir: str, prefix: str) -> str:
  date_str = date_utils.format_file_system_friendly_date(datetime.now())
  proposed_core_dir = f"{prefix}_{date_str}"
  proposed_dir = os.path.join(parent_dir, proposed_core_dir)
  count = 1
  while os.path.exists(proposed_dir):
    proposed_dir = os.path.join(parent_dir, f"{proposed_core_dir}-({count})")
    count += 1
    if count > 10:
      raise Exception("Something went wrong. Too many dirs with similar names.")

  os.makedirs(proposed_dir, exist_ok=False)
  return proposed_dir

def zip_dir(dir_to_zip, output_path):
  with ZipFile(output_path, 'x') as myzip:
    files = walk(dir_to_zip)
    for f in files:
      arcname = f'{f.replace(dir_to_zip, "")}'
      myzip.write(f, arcname=arcname)
    myzip.close()

  return output_path
