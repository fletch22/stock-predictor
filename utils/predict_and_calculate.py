import os

from stopwatch import Stopwatch

import config
from config import logger_factory

import statistics

from services.AutoMlPredictionService import AutoMlPredictionService
from utils import date_utils

logger = logger_factory.create_logger(__name__)

def predict_and_calculate():
  # short_model_id = "ICN7780367212284440071"  # fut_prof 68%
  # short_model_id = "ICN5723877521014662275" # closeunadj
  # short_model_id = "ICN2013469796611448097" # multi3 58.7%
  # short_model_id = "ICN7558148891127523672"  # multi3 57.5%
  # short_model_id = "ICN2174544806954869914"  # multi3 57.5%
  # short_model_id = "ICN2383257928398147071" # vol_eq 65%
  # short_model_id = "ICN200769567050768296"  # voleq_rec Sept %
  # short_model_id = "ICN1127515596385130617" # voleq 2018-06-18 66%
  # short_model_id = "ICN5283794452616644197" # volreq_fil_09_22_v20190923042914 7/17-8/30: 0.2637%
  # short_model_id = "ICN8748316622033513158" # fiddy with 19 vol acc:62
  # short_model_id = "ICN7150281105604556161" # deuce_eq min_price: mp: 5.0; mv: 2.79; sgp: 2.0; mv: 100000
  # short_model_id = "ICN6329887226779646397" # deuce 1.1%; mp: 5.0; mv: 2.79; sgp: 2.0; mv: 100000
  # short_model_id = "ICN323980058926307012" # tiny
  # short_model_id = "ICN1872651771008733456" # tin_big; limited set
  # short_model_id = "ICN7012160376262305340" # tin_big II;
  short_model_id = "ICN2140008179580940274" # millville_1:

  package_folders = [
    # "process_2019-09-09_22-14-56-0.37",  # vanilla chart 8-15-2019: -0.004937923352530126,
    # "process_2019-09-14_11-47-31-845.11",  # vol_eq
    # "process_2019-09-15_22-19-18-716.64",  # voleq_rec
    # "process_2019-09-20_16-33-46-522.36",  # 8-23 -0.0200  (without volatilty min)
    # "process_2019-09-20_15-07-42-224.54",  # 8-30  / 0.00528 (without volatilty min)
    # "process_2019-09-15_17-14-26-988.57",  # 8-30 .0026 / 0.00392 / .00572
    # "process_2019-09-15_17-10-30-37.45", # 8-29 .0030 / .00389 / 0.0077
    # "process_2019-09-15_17-06-36-672.94",  # 8-28 .0050  / .00441 / 0.0064
    # "process_2019-09-15_17-03-04-8.06", # 8-27 -0.0019 / .00324 / 0.0024
    # "process_2019-09-15_14-02-07-600.9",  # 8-23  -0.0120 / -0.0104 / -0.0174
    # "process_2019-09-15_13-58-15-340.5",  # 8-22   -0.0018 / -0.0004 / 0.0015
    # "process_2019-09-15_13-54-19-578.46",  # 8-21   0.0023 / 0.0049 / 0.01739
    # "process_2019-09-15_13-50-10-391.36",  # 8-20  -0.00198 / -0.0008 / -0.0021
    # "process_2019-09-15_13-45-52-578.17",  # 8-19   0.0063 / 0.0230 / 0.01038
    # "process_2019-09-15_13-39-32-870.98",  # 8-14  -0.0150 / -0.0142 / -0.0120
    # "process_2019-09-15_16-59-07-574.45",  # 8-09 -0.0038 / -0.0018 / -0.0014
    # "process_2019-09-15_16-55-31-225.99", # 8-08  0.0052 / 0.0069 / 0.0107
    # "process_2019-09-15_16-51-55-456.24",  # 8-07 -0.00094 / 0.00087 / 0.0014
    # "process_2019-09-15_16-48-14-245.38",  # 8-06 0.0063 / 0.0081 / 0.0102
    # "process_2019-09-15_16-44-44-97.67",  # 8-05 -0.0196 / -0.01789 / -0.0260
    # "process_2019-09-15_16-41-08-259.13",  # 8-02 -0.0057 / -0.0044 / -0.0009
    # "process_2019-09-15_16-37-38-928.1",  # 8-01 -0.0021 / -.000510995 / .000078447
    # "process_2019-09-15_16-34-06-17.38",  # 7-31 -0.00096 / 0.00187 /  0.0045
    # "process_2019-09-15_16-30-25-885.47",  # 7-30 0.0015 / 0.00236 / 0.0039
    # "process_2019-09-15_16-26-47-851.12",  # 7-29 -0.0018 / 0.000798 / 0.0006
    # "process_2019-09-15_16-23-03-394.13", # 7-26 0.00468 / 0.00422 / 0.00619
    # "process_2019-09-15_16-19-05-587.39",  # 7-25 -0.0062 / -0.00266 / 0.0023
    # "process_2019-09-15_16-15-23-25.45",  # 7-24  0.0022 / 0.00299 /  0.00051
    # "process_2019-09-15_16-11-40-753.51",  # 7-23  0.00349 / 0.00408 / 0.00577
    # "process_2019-09-15_16-07-42-827.66",  # 7-22  0.0015 / 0.01208 / 0.0267
    # "process_2019-09-15_16-04-00-588.58",  # 7-19  -0.0027 / -0.00149 / 0.0052
    # "process_2019-09-15_16-00-11-48.41",  # 7-18  -0.0012 / -0.00095 /  0.0039
    # "process_2019-09-15_15-56-00-356.66",  # 7-17  -0.0022 / -0.0011 / 0
    # "process_2019-09-29_13-18-36-138.66", # fiddy with 19 stdev (with std_min = 5.0:, volume 100k. price>50.0 .27%-.4%
    # "process_2019-10-05_20-26-42-487.36", # mp: 5.0; mv: 2.79; sgp: 2.0; mv: 100000
    # "process_2019-09-08_06-08-31-603.66", # 8-15-2019 24h multi3 57.5%
    # "process_2019-08-06_22-46-56-230.65", # vanilla chart
    # "process_2019-09-18_22-12-00-332.17", # voleq 06-18 66%
    # "process_2019-10-06_17-26-47-73.01", # tiny; smid: "ICN323980058926307012"; sgf: .01; mp: 5.0; score_threshold: .88; roi: 0.00455
    # "process_2019-10-07_22-13-33-758.15", # tin big; limited set; st: .50; roi: 1.7; st: 56: 2.39
    "process_2019-10-07_22-25-00-943.35", # milleville:
  ]

  # "process_2019-10-05_20-26-42-487.36",  # mp: 5.0; mv: 2.79; sgp: 2.0; mv: 100000
  #   sought_gain_frac = .01; score_threshold = .50; std_min = 2.79; min_price = 10.00; min_volume = 100000: roi: .125% - seems random
  # "process_2019-09-29_13-18-36-138.66",  # fiddy with 19 stdev (with std_min = 5.0:, volume 100k. price>50.0 .27%-.4%
  #   sought_gain_frac = .01; score_threshold = .50; std_min = 5.0; min_price = 20.00; min_volume = 100000: 95/10000; roi: .234%
  # "process_2019-10-07_22-13-33-758.15",
  #   tin big; limited set; st: .50; roi: .17%; st: 56: stm: 2.39
  #   tin big II; limited set; st: .80; 16/1000; roi: .60%; stm: 9999999:
  #   tin big II; limited set; st: .70; 82/1000; roi: .17%; stm: 10:
  # "process_2019-10-07_22-25-00-943.35":
  #   millville: ICN2140008179580940274: 1427/10000: mp: 5.0; st: .5; stdmin: 9999999; roi: .015-0.023
  #   millville: ICN2140008179580940274: 793/10000: mp: 5.0; st: .5; stdmin: 7.0; roi: 0.00307

  results = []
  for pf in package_folders:
    roi = pred_images_from_folder(short_model_id=short_model_id, package_folder=pf)
    results.append({'package_folder': pf, 'roi': roi})

  rois = []
  for r in results:
    rois.append(r['roi'])
    logger.info(f"{r['package_folder']}; roi: {r['roi']}")

  logger.info(f"Mean ROI: {round(statistics.mean(rois) * 100, 4)}%")


def pred_images_from_folder(short_model_id, package_folder):
  # Arrange
  data_cache_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", "SelectChartZipUploadService", package_folder)
  image_dir = os.path.join(data_cache_dir, "test_holdout")
  # image_dir = os.path.join(data_cache_dir, "graphed")

  sought_gain_frac = .01
  min_price = 5.00
  score_threshold = .50
  std_min = 99999.0
  min_volume = 1000
  max_files = 10000
  start_sample_date = None # date_utils.parse_std_datestring("2018-07-31")

  purge_cached = False


  auto_ml_service = AutoMlPredictionService(short_model_id, package_dir=data_cache_dir, score_threshold=score_threshold)

  # Act
  stopwatch = Stopwatch()
  stopwatch.start()
  roi = auto_ml_service.predict_and_calculate(task_dir=package_folder, image_dir=image_dir,
                                              min_price=min_price, min_volume=min_volume,
                                              sought_gain_frac=sought_gain_frac, std_min=std_min,
                                              max_files=max_files, purge_cached=purge_cached, start_sample_date=start_sample_date)
  stopwatch.stop()

  logger.info(f"Elapsed time: {round(stopwatch.duration / 60, 2)} minutes")

  return roi

if __name__ == "__main__":
  predict_and_calculate()