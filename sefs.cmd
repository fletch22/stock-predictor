# Collects and combines scores and bet data

SET UNIQIFY_LOG_PATH=true

call "activate.bat" stock-predictor & cd %~dp0 & python -m services.split_equity_fundy_service