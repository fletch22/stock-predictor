import os
from datetime import datetime

from stopwatch import Stopwatch

from charts.ChartType import ChartType
from config import logger_factory
from services.AutoMlPredictionService import AutoMlPredictionService
from services.SparkRenderImages import SparkRenderImages
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class PredictHoldoutDataService():

  @classmethod
  def get_predictions_on_holdout_date(cls):
    # short_model_id = "ICN7780367212284440071" # fut_prof 68%
    # short_model_id = "ICN7558148891127523672"  # multi_3 second one%
    # short_model_id = "ICN2174544806954869914"  # multi-2019-09-01 66%
    # short_model_id = "ICN2383257928398147071"  # vol_eq_09_14_11_47_31_845_11 65%
    short_model_id = "ICN200769567050768296"  # voleq_rec Sept %

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
      '2019-08-23',
    ]

    image_dirs = []
    for date_str in holdout_dates:
      dt = date_utils.parse_datestring(date_str)
      image_dirs.append(cls.get_predictions_on_date(dt, short_model_id, None))


  @classmethod
  def get_predictions_on_date(self, yield_date: datetime, short_model_id: str, start_sample_date: datetime):

    logger.info(f"Getting data for date {date_utils.get_standard_ymd_format(yield_date)}.")

    pct_gain_sought = 1.0
    num_days_to_sample = 1000
    score_threshold = .50
    sought_gain_frac = pct_gain_sought/100
    max_files = 3000
    min_price = 5.0
    amount_to_spend = 25000
    chart_type = ChartType.Neopolitan
    purge_cached = False
    volatility_min = 2.79

    stopwatch = Stopwatch()
    stopwatch.start()
    df, task_dir, image_dir = SparkRenderImages.render_train_test_day(yield_date, pct_gain_sought, num_days_to_sample, max_symbols=max_files,
                                                                      min_price=min_price, amount_to_spend=amount_to_spend, chart_type=chart_type,
                                                                      volatility_min=volatility_min)

    if df.shape[0] > 0:
      package_dir = os.path.dirname(image_dir)

      auto_ml_service = AutoMlPredictionService(short_model_id, package_dir=package_dir, score_threshold=score_threshold)
      auto_ml_service.predict_and_calculate(task_dir, image_dir, sought_gain_frac, max_files=max_files, purge_cached=purge_cached, start_sample_date=start_sample_date)
      stopwatch.stop()

      logger.info(f"Rendered images in {image_dir}")
      logger.info(f"Elapsed time: {stopwatch}.")
    else:
      logger.info(f"Something wrong. No data returned for date {date_utils.get_standard_ymd_format(yield_date)}.")

    return image_dir




if __name__ == "__main__":
  logger.info("Run from command line...")
  PredictHoldoutDataService.get_predictions_on_holdout_date()
