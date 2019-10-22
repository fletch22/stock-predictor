import os
from datetime import datetime

from stopwatch import Stopwatch

from categorical.BinaryCategoryType import BinaryCategoryType
from charts.ChartMode import ChartMode
from charts.ChartType import ChartType
from config.logger_factory import logger_factory
from prediction.PredictionRosebud import PredictionRosebud
from services.AutoMlPredictionService import AutoMlPredictionService
from services.SelectChartZipUploadService import SelectChartZipUploadService
from services.SlackService import SlackService
from utils import date_utils

logger = logger_factory.create_logger(__name__)


def predict(short_model_id: str, prediction_rosebud: PredictionRosebud, std_min:float):
  yield_date = prediction_rosebud.yield_date
  yield_date_str = date_utils.get_standard_ymd_format(yield_date)
  logger.info(f"Getting data for date {yield_date_str}.")

  stopwatch = Stopwatch()
  stopwatch.start()
  df, package_dir, image_dir = SelectChartZipUploadService.select_and_process_one_day(prediction_rosebud=prediction_rosebud)

  purge_cached = False

  pred_symbols = []
  if df.shape[0] > 0:
    max_files = prediction_rosebud.max_files

    auto_ml_service = AutoMlPredictionService(short_model_id, package_dir=package_dir, score_threshold=prediction_rosebud.score_threshold)
    results = auto_ml_service.predict(task_dir=package_dir, image_dir=image_dir,
                                      min_price=prediction_rosebud.min_price, min_volume=prediction_rosebud.min_volume,
                                      max_files=max_files, std_min=std_min,
                                      purge_cached=purge_cached, start_sample_date=None)
    logger.info(f"Rendered images in {image_dir}")

    pred_succ_list = [r for r in results if r['category_predicted'] == BinaryCategoryType.ONE]
    for r in pred_succ_list:
      symbol = r['symbol']
      pred_symbols.append(symbol)

    logger.info(f"{yield_date_str} Stock prediction: {pred_symbols}")
  else:
    logger.info(f"Something wrong. No data returned for date {date_utils.get_standard_ymd_format(prediction_rosebud.yield_date)}.")

  stopwatch.stop()
  logger.info(f"Elapsed time: {stopwatch}.")

  return df, package_dir, image_dir, pred_symbols


def current_rosebud_predictor():
  # short_model_id = "ICN5283794452616644197"  # volreq_fil_09_22_v20190923042914

  # short_model_id = "ICN8748316622033513158"  # fiddy; sgf = .01; st = .50; std_min = 5.0; mp = 20.00; min_volume = 100000: 278/10000; roi: .0014-.00187
    # sought_gain_frac = .01; score_threshold = .50; std_min = 5.0; min_price = 20.00; min_volume = 100000: 95/10000; roi: .234%
  # short_model_id = "ICN2140008179580940274" # 1427/10000: mp: 5.0; st: .5; std_min: 9999999; roi: .015-0.023
  short_model_id = "ICN1615151565178573620"  # bigname I:

  prediction_rosebud = PredictionRosebud()
  prediction_rosebud.pct_gain_sought = 1.0
  prediction_rosebud.num_days_to_sample = 1000
  prediction_rosebud.score_threshold = .90
  prediction_rosebud.sought_gain_frac = prediction_rosebud.pct_gain_sought / 100
  prediction_rosebud.max_files = 3000
  prediction_rosebud.min_volume = 0
  prediction_rosebud.min_price = 5.0
  prediction_rosebud.volatility_min = 99999999999.0
  prediction_rosebud.chart_type = ChartType.Neopolitan
  prediction_rosebud.chart_mode = ChartMode.Prediction
  prediction_rosebud.yield_date = datetime.now() # date_utils.parse_std_datestring('2019-09-27')
  prediction_rosebud.add_realtime_price_if_missing = True

  std_min = prediction_rosebud.volatility_min

  result, messages = prediction_rosebud.is_valid()
  if result is False:
    for msg in messages:
      logger.info(msg)

  df, task_dir, image_dir, pred_symbols = predict(short_model_id=short_model_id, prediction_rosebud=prediction_rosebud, std_min=std_min)

  logger.info(f"Created in {image_dir}.")

  slack_service = SlackService()

  if len(pred_symbols) > 0:
    message = ", ".join(pred_symbols)
  else:
    message = "None to recommend."

  slack_service.send_direct_message_to_chris(message)

  return pred_symbols

if __name__ == '__main__':
  current_rosebud_predictor()