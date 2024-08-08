from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

dt = sys.argv[1]
spark = SparkSession.builder.appName("Join").getOrCreate()

df = spark.read.parquet("/home/young12/data/movie/repartition")

df.write.partitionBy("load_dt", "multiMovieYn", "repNationCd").mode("overwrite").parquet("/home/young12/data/movie/hive/")

spark.stop()
