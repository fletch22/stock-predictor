import time

import schedule

from services import prediction_day_service, eod_data_service

schedule.every().day.at("07:30").do(eod_data_service.get_todays_merged_shar_data)
schedule.every().day.at("15:44").do(prediction_day_service.current_rosebud_predictor)

if __name__ == '__main__':
  while True:
    schedule.run_pending()
    time.sleep(1)