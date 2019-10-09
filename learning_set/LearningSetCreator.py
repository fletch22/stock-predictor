from datetime import datetime

from charts.ChartType import ChartType
from config.logger_factory import logger_factory
from services.SelectChartZipUploadService import SelectChartZipUploadService
from utils import date_utils
from learning_set.LearningSetMetaData import LearningSetMetaData

logger = logger_factory.create_logger(__name__)

class LearningSetCreator():

  @classmethod
  def create_learning_set_rosebud(self):

    lsm = LearningSetMetaData()
    lsm.min_price = 5.0
    lsm.trading_days_span = 1000
    lsm.min_samples = 100000
    lsm.pct_gain_sought = 1.0
    lsm.start_date = None  # date_utils.parse_datestring("2015-07-23")
    lsm.end_date = None # date_utils.parse_datestring("2019-07-17")
    lsm.pct_test_holdout = 10
    lsm.chart_type = ChartType.Neopolitan
    # 19.6511 is mean volatility for last 1000 trading days for < close price 50.00
    lsm.volatility_min = 99999999999
    lsm.min_volume = 1000000

    # lsm.min_price = 5.0
    # lsm.trading_days_span = 1000
    # lsm.min_samples = 10000
    # lsm.pct_gain_sought = 1.0
    # lsm.start_date = None  # date_utils.parse_datestring("2015-07-23")
    # lsm.end_date = date_utils.parse_std_datestring("2019-03-17")
    # lsm.pct_test_holdout = 10
    # lsm.chart_type = ChartType.Neopolitan
    # # 19.6511 is mean volatility for last 1000 trading days for < close price 50.00
    # lsm.volatility_min = 99999999999
    # lsm.min_volume = 10000

    test_train_dir = SelectChartZipUploadService.create_learning_set(lsm)

    logger.info(f"Train/Test Dir: {test_train_dir}")


if __name__ == '__main__':
  LearningSetCreator.create_learning_set_rosebud()