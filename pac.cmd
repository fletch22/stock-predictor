SET UNIQIFY_LOG_PATH=true

# python -m tests.realtime_agent.TestRealtimeAgent
# python -m pytest -s -v tests\services\TestEquityUtilService.py::TestEquityUtilService::test_foo
# python -m pytest -s tests\realtime_agent\TestRealtimeAgent.py::TestRealtimeAgent::test_model
python -m pytest -s tests\services\TestAutoMlPredictionService.py::TestAutoMlPredictionService::test_predict_and_calculate