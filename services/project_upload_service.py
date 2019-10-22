import math
import os
import shutil
from random import shuffle
from typing import List
import pandas as pd

import config
from categorical.BinaryCategoryType import BinaryCategoryType
from config.logger_factory import logger_factory
from services import file_services
from services.CloudFileService import CloudFileService
from utils import date_utils
from datetime import datetime

logger = logger_factory.create_logger(__name__)


def upload_images_and_meta_csv(package_folder: str, test_frac: float, validation_frac:float):
  files_to_delete, csv_path, gcs_package_path = upload_project(project_path=package_folder, test_frac=test_frac, validation_frac=validation_frac)

  gcs_meta_csv = f"{gcs_package_path}/{os.path.basename(csv_path)}"

  logger.info(f"Will attempt to upload the following:\n{gcs_meta_csv}\n")

  cloud_file_service = CloudFileService()
  cloud_file_service.upload_file(csv_path, gcs_meta_csv)

  return files_to_delete, csv_path, gcs_package_path, gcs_meta_csv

def get_date(file_path):
  file_info = file_services.get_filename_info(file_path)
  return date_utils.get_standard_ymd_format(file_info['date'])

def upload_project(project_path:str, test_frac:float, validation_frac:float):

  test_train_path = os.path.join(project_path, "train_test")

  path_zero = os.path.join(test_train_path, BinaryCategoryType.ZERO)
  path_one = os.path.join(test_train_path, BinaryCategoryType.ONE)

  files_zero = file_services.walk(path_zero)
  files_one = file_services.walk(path_one)

  train_frac = 1 - (test_frac + validation_frac)

  combined = files_zero + files_one

  comb_sorted = sorted(combined, key=get_date)

  total_sorted = len(comb_sorted)

  num_train = math.floor(total_sorted * train_frac)

  num_test_valid = math.ceil(total_sorted * (test_frac + validation_frac))
  train_files = comb_sorted[:num_train]
  test_validation_files = comb_sorted[-num_test_valid:]

  shuffle(test_validation_files)

  num_test = (math.floor(total_sorted * test_frac))
  num_validation = (math.floor(total_sorted * validation_frac))
  test_files = balance_by_symbol_and_cat(test_validation_files[-num_test:])
  validate_files = balance_by_symbol_and_cat(test_validation_files[:num_validation])
  train_files_bal = balance_files_by_category(train_files)

  backup_files(project_path, validate_files, "validation")
  backup_files(project_path, validate_files, "test")

  upload_project_folder = f"api_uploads/{os.path.basename(os.path.dirname(project_path))}_{date_utils.format_file_system_friendly_date(datetime.now())}"

  gcs_upload_prefix = f"gs://{config.constants.GCS_UPLOAD_BUCKET_NAME}/"
  gcs_package_path = f"{gcs_upload_prefix}{upload_project_folder}"

  upload_file_info = []
  build_upload_info(gcs_package_path, train_files_bal, 'TRAIN', upload_file_info)
  build_upload_info(gcs_package_path, test_files, 'TEST', upload_file_info)
  build_upload_info(gcs_package_path, validate_files, 'VALIDATION', upload_file_info)

  csv_path = os.path.join(project_path, "gcs_upload_meta.csv")
  df = pd.DataFrame(upload_file_info)
  df.drop(['local_file_path'], axis=1)
  column_order = ['activity_label', 'cloud_path', 'label']
  df = df[column_order]
  df['bounding_box_coor_0'] = ""
  df['bounding_box_coor_1'] = ""
  df['bounding_box_coor_2'] = ""
  df['bounding_box_coor_3'] = ""
  df['bounding_box_coor_4'] = ""
  df['bounding_box_coor_5'] = ""
  df['bounding_box_coor_6'] = ""
  df['bounding_box_coor_7'] = ""
  df['bounding_box_coor_8'] = ""

  df.to_csv(csv_path, index=False, header=False)

  cloud_file_service = CloudFileService()

  delete_cloud_files = []
  gcs_uploads = []
  for file_info in upload_file_info:
    cloud_path = file_info['cloud_path']
    if cloud_path.startswith(gcs_upload_prefix):
      cloud_path = cloud_path[len(gcs_upload_prefix):]
    file_path = file_info['local_file_path']
    delete_cloud_files.append(cloud_path)
    gcs_uploads.append({
      "source_file_path": file_path,
      "destination_blob_name": cloud_path
    })

  cloud_file_service.upload_multi(gcs_uploads, gcs_package_path)

  return delete_cloud_files, csv_path, upload_project_folder


def backup_files(project_path, files, backup_folder_name:str):
  backup_path = os.path.join(project_path, backup_folder_name)
  os.makedirs(backup_path, exist_ok=True)
  for f in files:
    shutil.copy(f, os.path.join(backup_path, os.path.basename(f)))


def build_upload_info(gcs_package_path: str, file_paths: List, activity_label: str, collector: List):
  for ndx, item in enumerate(file_paths):
    file_info = file_services.get_filename_info(item)
    category = file_info['category']
    filename = f"{ndx}-{activity_label}{os.path.splitext(item)[1]}"
    cloud_file_path = f"{gcs_package_path}/{category}/{os.path.basename(filename)}"
    collector.append({"activity_label": activity_label, 'cloud_path': cloud_file_path, 'label': category, 'local_file_path': item})

def balance_files_by_category(files: List[str]):
  files_zero = [f for f in files if os.path.basename(f).startswith(BinaryCategoryType.ZERO)]
  files_one = [f for f in files if os.path.basename(f).startswith(BinaryCategoryType.ONE)]

  num_zero = len(files_zero)
  num_one = len(files_one)

  diff = num_zero - num_one
  if diff > 0:
    files_zero = files_zero[:num_one]
  else:
    files_one = files_one[:num_zero]

  return files_zero + files_one

def balance_by_symbol_and_cat(files: List):
  symb_dict = {}
  for f in files:
    symbol = file_services.get_filename_info(f)['symbol']

    symb_files = []
    if symbol in symb_dict.keys():
      symb_files = symb_dict[symbol]
    else:
      symb_dict[symbol] = symb_files

    symb_files.append(f)

  results = []
  for s in symb_dict.keys():
    symb_files = symb_dict[s]
    results.extend(balance_files_by_category(symb_files))

  return results
