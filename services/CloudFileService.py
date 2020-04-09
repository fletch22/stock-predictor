import time
from typing import Dict, List

import findspark
from google.cloud import storage
from google.oauth2 import service_account
from ipython_genutils.py3compat import xrange
from pyspark import SparkContext, SparkFiles
from itertools import zip_longest

import config
from config import logger_factory
from services import file_services

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

  def list_filenames(self, path_dir_after_bucket_name: str):
    blobs = self.client.list_blobs(self._get_upload_bucket(), prefix=path_dir_after_bucket_name)

    return [b.name for b in blobs]

  def upload_multi(self, upload_file_infos: List[Dict], gs_package_path: str):
    upload_needed = self.filter_out_unneeded(upload_file_infos, gs_package_path)

    num_already_uploaded = len(upload_file_infos) - len(upload_needed)
    if num_already_uploaded > 0:
      logger.info(f"Found {num_already_uploaded} files already uploaded. Skipping those.")

    if len(upload_needed) > 0:
      get_spark_uploaders(upload_needed)

  def filter_out_unneeded(self, upload_file_infos, destination_cloud_folder_path):
    file_already_uploaded = self.list_filenames(destination_cloud_folder_path)

    return [ufi for ufi in upload_file_infos if ufi["destination_blob_name"] not in file_already_uploaded]

  def sync_folders(self, local_source_path: str, remote_gcs_folder):
    files = file_services.walk(local_source_path)

    for f in files:
      pass

def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)

def get_spark_uploaders(upload_file_infos):
  sublist_size = (len(upload_file_infos) // 20000) + 1
  logger.info(f"Sublist size: {sublist_size}")
  chunks = [upload_file_infos[x:x + sublist_size] for x in xrange(0, len(upload_file_infos), sublist_size)]

  logger.info(f"Upload in {len(chunks)} chunks; each chunk size: {sublist_size}.")

  smaller_chunks = grouper(chunks, 200)
  findspark.init()
  sc = SparkContext.getOrCreate()
  sc.setLogLevel("INFO")
  print(sc._jsc.sc().uiWebUrl().get())
  for small_chunk in smaller_chunks:
    filtered = [i for i in small_chunk if i]

    rdd = sc.parallelize(filtered)
    rdd.foreach(spark_gcs_upload)

    sc = SparkContext.getOrCreate()
    sc.setLogLevel("INFO")

  sc.stop()

def spark_gcs_upload(upload_file_infos):
  cloud_file_services = CloudFileService()

  if upload_file_infos is not None:
    for file_info in upload_file_infos:
      file_path = SparkFiles.get(file_info["source_file_path"])
      destination_blob = file_info["destination_blob_name"]

      upload_file_retry(cloud_file_services, file_path, destination_blob)


def upload_file_retry(cloud_file_services: CloudFileService, file_path, destination_blob):
  max_retries = 3
  is_sent = False
  count = 0
  pause_time = 1
  while is_sent is False and count < max_retries:
    try:
      logger.info(f"Send file {destination_blob}.")
      cloud_file_services.upload_file(file_path, destination_blob)
      is_sent = True
    except:
      logger.info(f"Error sending! Pausing {pause_time} seconds for effect.")
      time.sleep(pause_time)
      count += 1
      if count > max_retries:
        logger.info(f"Error sending! Reached maximum {max_retries} send retries. Giving up attempting to send {destination_blob}.")


