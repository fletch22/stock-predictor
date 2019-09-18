
import findspark
from pyspark import SparkContext

from services.SparkChartService import SparkChartService

def do_spark(stock_infos):
  findspark.init()
  sc = SparkContext.getOrCreate()
  sc.setLogLevel("INFO")
  print(sc._jsc.sc().uiWebUrl().get())

  rdd = sc.parallelize(stock_infos)

  spark_chart_service = SparkChartService()
  rdd.foreach(spark_chart_service.process)

  sc.stop()

