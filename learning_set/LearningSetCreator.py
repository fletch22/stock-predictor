from datetime import datetime

from stopwatch import Stopwatch

from charts.ChartType import ChartType
from config.logger_factory import logger_factory
from services import project_upload_service
from services.SelectChartZipUploadService import SelectChartZipUploadService
from learning_set.LearningSetMetaData import LearningSetMetaData
from utils import date_utils

logger = logger_factory.create_logger(__name__)

class LearningSetCreator():

  @classmethod
  def create_learning_set_rosebud(self):

    stopwatch = Stopwatch()
    stopwatch.start()

    lsm = LearningSetMetaData()
    lsm.min_price = 5.0
    lsm.trading_days_span = 1000
    lsm.min_samples = 500000
    lsm.pct_gain_sought = 1.0
    lsm.start_date = None  # date_utils.parse_datestring("2015-07-23")
    lsm.end_date = date_utils.parse_std_datestring("2018-11-15")
    lsm.pct_test_holdout = 10
    lsm.chart_type = ChartType.Neopolitan
    # 19.6511 is mean volatility for last 1000 trading days for < close price 50.00
    lsm.volatility_min = 99999999999
    lsm.min_volume = 0
    lsm.symbol_focus = ['AAPL', 'ABEV', 'ACB', 'AGNC', 'AMAT', 'AMD', 'AMRH', 'AMRN', 'ATVI', 'AUY', 'BABA', 'BAC', 'BANT', 'BBD', 'BHGE', 'C', 'CELG', 'CHK', 'CMCSA', 'COTY', 'CSCO', 'CZR', 'DELL', 'DNR', 'EBAY', 'ECA', 'EQT', 'F', 'FB', 'FCX', 'FDC', 'FMCC', 'FNMA', 'GE', 'GGB', 'GM', 'GOLD', 'HAL', 'HBAN', 'HMNY', 'HPE', 'INTC', 'ITUB', 'JCP', 'JD', 'JPM', 'KBH', 'KEY', 'KGC', 'KMI', 'KO', 'LYG', 'M', 'MO', 'MRK', 'MRO', 'MRVL', 'MS', 'MSFT', 'MU', 'NBEV', 'NBR', 'NIO', 'NLY', 'NOK', 'NTAP', 'NVDA', 'NWL', 'ORCL', 'PBR', 'PCG', 'PFE', 'PG', 'PPL', 'QCOM', 'RF', 'RIG', 'S', 'SBUX', 'SIRI', 'SLB', 'SNAP', 'SQ', 'SWN', 'SYMC', 'T', 'TAHO', 'TEVA', 'TSM', 'TWTR', 'VALE', 'VER', 'VICI', 'VIPS', 'VZ', 'WFC', 'WFTIQ', 'WMT', 'XOM', 'ZNGA']

    package_path, test_train_dir, holdout_dir = SelectChartZipUploadService.create_learning_set(lsm)
    test_frac = .1
    validation_frac = .1

    created_files, csv_path, gcs_package_path, gcs_meta_csv = project_upload_service.upload_images_and_meta_csv(package_folder=package_path,
                                                                                                                test_frac=test_frac,
                                                                                                                validation_frac=validation_frac)

    total_files = len(created_files)
    logger.info(f"Train/Test Dir: {test_train_dir}: Total files {total_files}.")

    stopwatch.stop()
    logger.info(f"Elapsed time: {round(stopwatch.duration, 2)}.")

if __name__ == '__main__':
  LearningSetCreator.create_learning_set_rosebud()