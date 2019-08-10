import os
import time
from unittest import TestCase

import findspark
import pandas as pd
from pyspark import SparkContext, SparkFiles
from pyspark.sql import SQLContext, DataFrame
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
from pyspark.sql.types import LongType, DoubleType, StructField, StructType, FloatType

import config
from config import logger_factory
from services import chart_service, file_services
from services.Eod import Eod
from services.EquityUtilService import EquityUtilService
from services.SampleFileTypeSize import SampleFileTypeSize
from services.StockService import StockService

logger = logger_factory.create_logger(__name__)

class TestPySpark(TestCase):

  def test_connection(self):
    # Arrange
    findspark.init()
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)

    # Act
    df: DataFrame = sqlContext.read.format('com.databricks.spark.csv')\
      .options(header='true', inferschema='true').load(config.constants.SHAR_EQUITY_PRICES_SHORT)

    # Assert
    sc.stop()



  



