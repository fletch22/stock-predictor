SET UNIQIFY_LOG_PATH=true

python -c "from services import eod_data_service; eod_data_service.get_todays_merged_shar_data()"