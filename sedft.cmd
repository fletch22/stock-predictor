
SET UNIQIFY_LOG_PATH=true

call "activate.bat" stock-predictor & cd %~dp0 & python -m services.split_eod_data_to_files_service

