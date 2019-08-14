import os

from stopwatch import Stopwatch

from config import logger_factory
from services.AutoMlPredictionService import AutoMlPredictionService
from services.EquityUtilService import EquityUtilService
from services.SparkRenderImages import SparkRenderImages
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class ControlPanelService():

  @classmethod
  def get_predictions_on_holdout_date(self):
    yield_date = date_utils.parse_datestring("2019-07-26")
    pct_gain_sought = 1.0
    num_days_to_sample = 1000
    # short_model_id = "ICN5723877521014662275"
    short_model_id = "ICN7780367212284440071" # fut_prof 68%
    score_threshold = .50
    sought_gain_frac = .01
    max_files = 3000
    purge_cached = False

    stopwatch = Stopwatch()
    stopwatch.start()
    df, image_dir = SparkRenderImages.render_train_test_day(yield_date, pct_gain_sought, num_days_to_sample)
    package_dir = os.path.dirname(image_dir)

    auto_ml_service = AutoMlPredictionService(short_model_id, package_dir=package_dir, score_threshold=score_threshold)
    auto_ml_service.predict_and_calculate(image_dir, sought_gain_frac, max_files=max_files, purge_cached=purge_cached)
    stopwatch.stop()

    logger.info(f"Rendered images in {image_dir}")
    logger.info(f"Elapsed time: {stopwatch}.")

    return image_dir

  @classmethod
  def split_files(cls):
    stopwatch = Stopwatch()
    stopwatch.start()
    stock_infos = EquityUtilService.split_shar_equity_to_ticker_files()
    stopwatch.stop()

    logger.info(f"Elapsed: {stopwatch}.")




if __name__ == "__main__":
  logger.info("Run from command line...")
  ControlPanelService.get_predictions_on_holdout_date()
