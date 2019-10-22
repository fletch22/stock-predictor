import os
import shutil

import math
import pandas as pd
from datetime import datetime

from google.cloud import automl_v1beta1 as automl

import config
from categorical.BinaryCategoryType import BinaryCategoryType
from config import logger_factory
from services import file_services
from utils import date_utils

logger = logger_factory.create_logger(__name__)


class AutoMlGeneralService():
  client = None
  project_id = None
  location = None

  def __init__(self, project_id: str, location: str):
    self.client = automl.AutoMlClient.from_service_account_file(config.constants.CREDENTIALS_PATH)
    self.project_id = project_id
    self.location = location

  def create_dataset(self, dataset_name: str):
    parent = self.client.location_path(self.project_id, self.location)

    # Classification type is assigned based on multilabel value.
    # Specify the image classification type for the dataset.
    dataset_metadata = {"classification_type": "MULTICLASS"}

    # Set dataset name and metadata of the dataset.
    dataset = {
      "display_name": dataset_name,
      "image_classification_dataset_metadata": dataset_metadata,
    }

    return self.client.create_dataset(parent, dataset)

  def import_data(self, dataset_name: str, bucket_name: str, cloud_file_path: str):
    # gs://api_uploads/process_2019-07-28_16-43-07-519.98_train_test_for_upload.zip
    input_config = {
      "gcs_source": {
        "input_uris": [
          f"gs://{bucket_name}/{cloud_file_path}"
        ]
      }
    }

    name = self.client.dataset_path(self.project_id, self.location, dataset_name)

    response = self.client.import_data(name, input_config)

    def callback(operation_future):
      result = operation_future.result()

    response.add_done_callback(callback)

    # Handle metadata.
    metadata = response.metadata()

    return metadata

  def upload_batch(self, file_uploads: list):
    pass

  # NOTE: 2019-08-05: Some stocks follow the S&P closely. If you choose 2 stocks at the same moment of time, train on one and test on the other, you
  # will likely get an accurate prediction; but this is cheating. We can only train on items in the past, not contemporaneous with the test selection.
  # If we did that we would merely be confirming that the present allows us to predict the present.
  @classmethod
  def sort_by_date_in_basename(cls, files_filtered):

    def get_date_str(file_path):
      date: datetime = file_services.get_filename_info(file_path)["date"]

      return date_utils.get_standard_ymd_format(date)

    return sorted(files_filtered, key=lambda file_path: get_date_str(file_path))

  @classmethod
  def prep_for_upload(cls, prediction_dir: str, num_files_needed: int):
    parent_dir = os.path.join(prediction_dir, "graphed")

    categories = [BinaryCategoryType.ONE, BinaryCategoryType.ZERO]
    files_needed = math.ceil(num_files_needed/2)
    logger.info(f"files needed {files_needed}; parent_dir: {parent_dir}")

    test_dir = os.path.join(prediction_dir, "test_holdout")
    os.makedirs(test_dir, exist_ok=True)

    train_test_dir = os.path.join(prediction_dir, "train_test")
    os.makedirs(train_test_dir, exist_ok=True)

    def move(file_list, cat_dir, hide_details: bool):
      for ndx, f in enumerate(file_list):
        _, file_extension = os.path.splitext(f)

        if hide_details:
          filename = f"{ndx}{file_extension}"
        else:
          filename = os.path.basename(f)

        dest_path = os.path.join(cat_dir, filename)
        logger.info(f"Moving {f} to {dest_path}")
        shutil.move(f, dest_path)

    for cat in categories:
      files_raw = file_services.walk(parent_dir)
      files_filtered = [f for f in files_raw if os.path.basename(f).startswith(f"{cat}_")]
      files_filtered = cls.sort_by_date_in_basename(files_filtered)

      train_test_files = files_filtered[:-files_needed]
      test_holdout_files = files_filtered[-files_needed:]

      cat_dir_test = os.path.join(test_dir, cat)
      os.makedirs(cat_dir_test, exist_ok=True)

      cat_dir_train = os.path.join(train_test_dir, cat)
      os.makedirs(cat_dir_train, exist_ok=True)

      move(test_holdout_files, cat_dir_test, False)
      move(train_test_files, cat_dir_train, False)

    return train_test_dir, test_dir

  @classmethod
  def split_files(cls, cat, files_filtered, files_needed, test_dir, train_test_dir):
    test_holdout_files = files_filtered[files_needed:]
    train_test_files = files_filtered[:files_needed]

    cat_dir_test = os.path.join(test_dir, cat)
    os.makedirs(cat_dir_test, exist_ok=True)

    cat_dir_train = os.path.join(train_test_dir, cat)
    os.makedirs(cat_dir_train, exist_ok=True)

    return cat_dir_test, cat_dir_train, test_holdout_files, train_test_files

