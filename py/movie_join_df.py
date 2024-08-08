from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

LOAD_DT = sys.argv[1]
spark = SparkSession.builder.appName("Join").getOrCreate()

df1 = spark.read.parquet(f"/home/young12/data/movie/repartition/load_dt={LOAD_DT}")

df1.createOrReplaceTempView("movie")

df2 = spark.sql(f"""
SELECT 
    movieCd,
    movieNm,
    salesAmt,
    audiCnt,
    showCnt,
    multiMovieYn,
    repNationCd,
    '{LOAD_DT}' AS load_dt
FROM movie
WHERE multiMovieYn IS NULL
""")

df2.createOrReplaceTempView("multi_null")

df3 = spark.sql(f"""
SELECT 
    movieCd, 
    movieNm,
    salesAmt,
    audiCnt,
    showCnt,
    multiMovieYn,
    repNationCd,
    '{LOAD_DT}' AS load_dt
FROM movie
WHERE repNationCd IS NULL
""")

df3.createOrReplaceTempView("nation_null")

df_j = spark.sql(f"""
SELECT
    COALESCE(m.movieCd, n.movieCd) AS movieCd,
    COALESCE(m.salesAmt, n.salesAmt),
    COALESCE(m.audiCnt, n.audiCnt),
    COALESCE(m.showCnt, n.showCnt),
    multiMovieYn,
    repNationCd, 
    '{LOAD_DT}' AS load_dt
FROM multi_null m FULL OUTER JOIN nation_null n
ON m.movieCd = n.movieCd""")

df_j.createOrReplaceTempView("join_df")

df_j.write.partitionBy("load_dt", "multiMovieYn", "repNationCd").parquet("/home/young12/data/movie/hive/")

spark.stop()
