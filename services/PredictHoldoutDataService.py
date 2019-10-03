import os
from datetime import datetime

from stopwatch import Stopwatch

from charts.ChartMode import ChartMode
from charts.ChartType import ChartType
from config import logger_factory
from prediction.PredictionRosebud import PredictionRosebud
from services import prediction_render_service
from services.AutoMlPredictionService import AutoMlPredictionService
from services.SelectChartZipUploadService import SelectChartZipUploadService
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class PredictHoldoutDataService():

  @classmethod
  def get_predictions_on_holdout_date(cls):
    # short_model_id = "ICN7780367212284440071" # fut_prof 68%
    # short_model_id = "ICN7558148891127523672"  # multi_3 second one%
    # short_model_id = "ICN2174544806954869914"  # multi-2019-09-01 66%
    # short_model_id = "ICN2383257928398147071"  # vol_eq_09_14_11_47_31_845_11 65%
    # short_model_id = "ICN200769567050768296"  # voleq_rec Sept %
    short_model_id = "ICN5283794452616644197" # volreq_fil_09_22_v20190923042914

    # holdout_dates = [
    #   '2019-07-17',
    #   '2019-07-18',
    #   '2019-07-19',
    #   '2019-07-22',
    #   '2019-07-23',
    #   '2019-07-24',
    #   '2019-07-25',
    #   '2019-07-26',
    #   '2019-07-29',
    #   '2019-07-30',
    #   '2019-07-31',
    #   '2019-08-01',
    #   '2019-08-02',
    #   '2019-08-05',
    #   '2019-08-06',
    #   '2019-08-07',
    #   '2019-08-08',
    #   '2019-08-09',
    #   '2019-08-27',
    #   '2019-08-28',
    #   '2019-08-29',
    #   '2019-08-30',
    #   '2019-09-02',
    #   '2019-09-03',
    #   '2019-09-04',
    #   '2019-09-05',
    #   '2019-09-06'
    # ]

    holdout_dates = [
      '2019-08-28'
    ]

    image_dirs = []
    for date_str in holdout_dates:
      dt = date_utils.parse_std_datestring(date_str)
      image_dirs.append(cls.get_predictions_on_date(dt, short_model_id, None, add_realtime_price_if_missing=False))


  @classmethod
  def get_predictions_on_date(self, yield_date: datetime, short_model_id: str, start_sample_date: datetime, add_realtime_price_if_missing: bool):

    logger.info(f"Getting data for date {date_utils.get_standard_ymd_format(yield_date)}.")

    prediction_rosebud = PredictionRosebud()
    prediction_rosebud.pct_gain_sought = 1.0
    prediction_rosebud.num_days_to_sample = 1000
    prediction_rosebud.score_threshold = .50
    prediction_rosebud.sought_gain_frac = prediction_rosebud.pct_gain_sought / 100
    prediction_rosebud.max_files = 3000
    prediction_rosebud.min_price = 5.0
    prediction_rosebud.amount_to_spend = 25000
    prediction_rosebud.chart_type = ChartType.Neopolitan
    prediction_rosebud.chart_mode = ChartMode.BackTest
    prediction_rosebud.volatility_min = 2.79
    prediction_rosebud.yield_date = yield_date
    prediction_rosebud.add_realtime_price_if_missing = add_realtime_price_if_missing

    std_min = 2.0

    stopwatch = Stopwatch()
    stopwatch.start()
    df, task_dir, image_dir = SelectChartZipUploadService.select_and_process_one_day(prediction_rosebud=prediction_rosebud)

    purge_cached = False

    if df.shape[0] > 0:
      package_dir = os.path.dirname(image_dir)

      auto_ml_service = AutoMlPredictionService(short_model_id, package_dir=package_dir, score_threshold=prediction_rosebud.score_threshold)
      auto_ml_service.predict_and_calculate(task_dir, image_dir, prediction_rosebud.sought_gain_frac, std_min=std_min, max_files=prediction_rosebud.max_files, purge_cached=purge_cached, start_sample_date=start_sample_date)
      logger.info(f"Rendered images in {image_dir}")
    else:
      logger.info(f"Something wrong. No data returned for date {date_utils.get_standard_ymd_format(prediction_rosebud.yield_date)}.")

    stopwatch.stop()
    logger.info(f"Elapsed time: {stopwatch}.")

    return image_dir




if __name__ == "__main__":
  logger.info("Run from command line...")
  PredictHoldoutDataService.get_predictions_on_holdout_date()
