from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

LOAD_DT = sys.argv[1]
spark = SparkSession.builder.appName("agg").getOrCreate()

df1 = spark.read.parquet(f"/home/young12/data/movie/hive/load_dt={LOAD_DT}")
df1.createOrReplaceTempView("movie")

sum_m = spark.sql(f"""
SELECT
    sum(salesAmt) as sum_salesAmt,
    sum(audiCnt) as sum_audiCnt,
    sum(showCnt) as sum_showCnt,
    multiMovieYn,
    '{LOAD_DT}' AS load_dt
FROM movie
GROUP BY multiMovieYn
""")

sum_n = spark.sql(f"""
SELECT
    sum(salesAmt) as sum_salesAmt,
    sum(audiCnt) as sum_audiCnt,
    sum(showCnt) as sum_showCnt,
    repNationCd,
    '{LOAD_DT}' AS load_dt
FROM movie
GROUP BY repNationCd
""")

sum_m.write.mode('append').partitionBy("load_dt").parquet(f"/home/young12/data/movie/sum-multi")
sum_n.write.mode('append').partitionBy("load_dt").parquet(f"/home/young12/data/movie/sum-nation")

spark.stop()

