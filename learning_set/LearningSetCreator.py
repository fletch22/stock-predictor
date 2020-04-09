from stopwatch import Stopwatch

from charts.ChartType import ChartType
from config.logger_factory import logger_factory
from services import project_upload_service
from services.SelectChartZipUploadService import SelectChartZipUploadService
from learning_set.LearningSetMetaData import LearningSetMetaData
logger = logger_factory.create_logger(__name__)

class LearningSetCreator():

  @classmethod
  def create_learning_set_rosebud(self):

    stopwatch = Stopwatch()
    stopwatch.start()

    lsm = LearningSetMetaData()
    lsm.min_price = 5.0
    lsm.trading_days_span = 200
    lsm.min_samples = 1200000
    lsm.pct_gain_sought = 1.0
    lsm.start_date = None # date_utils.parse_std_datestring("2015-06-17")
    lsm.end_date = None # date_utils.parse_std_datestring("2019-08-16")
    lsm.pct_test_holdout = 5
    lsm.chart_type = ChartType.Vanilla
    # 19.6511 is mean volatility for last 1000 trading days for < close price 50.00
    lsm.volatility_min = 99999999999
    lsm.min_volume = 0
    lsm.symbol_focus = None # ['AAPL', 'ABBV', 'ABEV', 'ACB', 'AKS', 'AMD', 'APHA', 'APRN', 'AR', 'AUY', 'AVP', 'BABA', 'BAC', 'BMY', 'C', 'CGIX', 'CHK', 'CLDR', 'CLF', 'CMCSA', 'CPE', 'CRZO', 'CSCO', 'CTL', 'CY', 'CZR', 'DAL', 'DB', 'DNR', 'ECA', 'ET', 'F', 'FB', 'FCEL', 'FCX', 'FHN', 'GBTC', 'GE', 'GGB', 'GM', 'GNMX', 'GOLD', 'HAL', 'HL', 'HSGX', 'IMRN', 'INFY', 'INTC', 'ITUB', 'JD', 'JNJ', 'JPM', 'KEY', 'KMI', 'MGTI', 'MPW', 'MRO', 'MS', 'MSFT', 'MSRT', 'MU', 'NBR', 'NIO', 'NLY', 'NOK', 'ORCL', 'OXY', 'PBCT', 'PBR', 'PDD', 'PFE', 'QCOM', 'RF', 'RIG', 'ROKU', 'RRC', 'S', 'SAN', 'SCHW', 'SIRI', 'SLB', 'SLS', 'SNAP', 'SWN', 'SYMC', 'T', 'TEVA', 'TRNX', 'TRQ', 'TWTR', 'UBNK', 'VALE', 'VZ', 'WDC', 'WFC', 'WMB', 'WPX', 'X', 'XOM', 'ZNGA']

    package_path, test_train_dir, holdout_dir = SelectChartZipUploadService.create_learning_set(lsm)
    test_frac = .2
    validation_frac = .2

    # package_path = "C:\\Users\\Chris\\workspaces\\data\\financial\\output\\stock_predictor\\selection_packages\\SelectChartZipUploadService\\process_2019-11-14_08-41-16-223.75"

    created_files, csv_path, gcs_package_path, gcs_meta_csv = project_upload_service.upload_images_and_meta_csv(package_folder=package_path,
                                                                                                                test_frac=test_frac,
                                                                                                                validation_frac=validation_frac)

    total_files = len(created_files)
    logger.info(f"Train/Test Dir: {test_train_dir}: Total files {total_files}.")

    stopwatch.stop()
    logger.info(f"Elapsed time: {round(stopwatch.duration, 2)}.")

if __name__ == '__main__':
  LearningSetCreator.create_learning_set_rosebud()