from unittest import TestCase

from charts.ChartMode import ChartMode
from charts.ChartType import ChartType
from config import logger_factory
from prediction.PredictionRosebud import PredictionRosebud
from services import file_services
from services.SelectChartZipUploadService import SelectChartZipUploadService
from utils import date_utils

logger = logger_factory.create_logger(__name__)


class TestSelectChartZipUploadService(TestCase):

  def test_create_daily_set(self):
    # Arrange
    prediction_rosebud = PredictionRosebud()
    prediction_rosebud.pct_gain_sought = 1.0
    prediction_rosebud.num_days_to_sample = 1000
    prediction_rosebud.score_threshold = .50
    prediction_rosebud.sought_gain_frac = prediction_rosebud.pct_gain_sought / 100
    prediction_rosebud.max_files = 1000
    prediction_rosebud.min_price = 5.0
    prediction_rosebud.amount_to_spend = 25000
    prediction_rosebud.chart_type = ChartType.Neopolitan
    prediction_rosebud.chart_mode = ChartMode.BackTest
    prediction_rosebud.volatility_min = 2.80
    prediction_rosebud.yield_date = date_utils.parse_datestring("2019-07-18")
    prediction_rosebud.add_realtime_price_if_missing = False

    df, package_path, image_dir = SelectChartZipUploadService.select_and_process_one_day(prediction_rosebud=prediction_rosebud)

    # Assert
    num_files = len(file_services.walk(image_dir))
    num_records = len(df["ticker"].unique().tolist())

    assert (num_files == num_records)
