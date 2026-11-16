"""Register a disposable application until a guard stops it through the master."""

import sys
import time

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(sys.argv[1]).getOrCreate()
try:
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline and not spark.sparkContext._jsc.sc().isStopped():
        time.sleep(0.2)
finally:
    spark.stop()
