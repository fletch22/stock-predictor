from google.cloud import storage
from google.oauth2 import service_account

import config
from config import logger_factory

logger = logger_factory.create_logger(__name__)

class CloudFileService():
  client = None

  def __init__(self):
    credentials = service_account.Credentials.from_service_account_file(config.constants.CREDENTIALS_PATH)
    self.client = storage.Client(project="fletch22-ai", credentials=credentials)

  def upload_file(self, source_file_name, destination_blob_name):
    blob = self._get_upload_bucket().blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    logger.info(f"File {source_file_name} uploaded to {destination_blob_name}.")

    return

  def delete_file_from_uploads(self, blob_name):
    blob = self._get_upload_bucket().blob(blob_name)
    blob.delete()

  def _get_upload_bucket(self):
    return self.client.get_bucket(config.constants.GCS_UPLOAD_BUCKET_NAME)

  def file_exists_in_uploads(self, file_path):
    blob = self._get_upload_bucket().blob(file_path)
    return blob.exists()
