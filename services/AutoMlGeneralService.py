import os
import random

from google.cloud import automl_v1beta1 as automl

import config
from config import logger_factory
from services import file_services
from services.StockService import StockService
from utils import date_utils
from statistics import mean

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