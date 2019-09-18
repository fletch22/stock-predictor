import os
from datetime import datetime

from charts.ChartType import ChartType
from config import logger_factory
from services.SelectChartZipUploadService import SelectChartZipUploadService

logger = logger_factory.create_logger(__name__)

class SparkRenderImages():

  @classmethod
  def render_train_test_day(cls, yield_date: datetime, pct_gain_sought:float, num_days_to_sample: int, max_symbols: int,
                            min_price: float, amount_to_spend: float, chart_type: ChartType, volatility_min: float):
    df_g_filtered, package_path = SelectChartZipUploadService.select_and_process_one_day(min_price=min_price,
                                                     amount_to_spend=amount_to_spend,
                                                     trading_days_span=num_days_to_sample,
                                                     min_samples=max_symbols,
                                                     pct_gain_sought=pct_gain_sought,
                                                     yield_date=yield_date, chart_type=chart_type, volatility_min=volatility_min)

    image_dir = os.path.join(package_path, 'graphed')

    return df_g_filtered, package_path, image_dir
