from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

dt = sys.argv[1]
spark = SparkSession.builder.appName("Join").getOrCreate()

df1 = spark.read.parquet("/home/young12/data/movie/repartition/load_dt={load_dt}")

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
    -- repNationCd,
    '{LOAD_DT}' AS load_dt
FROM movie
WHERE repNationCd IS NULL
""")

df3.createOrReplaceTempView("nation_null")

df_j = spark.sql(f"""
SELECT
    COALESCE(m.movieCd, n.movieCd) AS movieCd,
    COALESCE(m.salesAmt, n.salesAmt), -- 매출액
    COALESCE(m.audiCnt, n.audiCnt), -- 관객수
    COALESCE(m.showCnt, n.showCnt), --- 사영횟수
    multiMovieYn, -- 다양성 영화/상업영화를 구분지어 조회할 수 있습니다. “Y” : 다양성 영화 “N”
    repNationCd, -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
    '{LOAD_DT}' AS load_dt
FROM multi_null m FULL OUTER JOIN nation_null n
ON m.movieCd = n.movieCd""")

df_j.createOrReplaceTempView("join_df")

df.write.partitionBy("load_dt", "multiMovieYn", "repNationCd").parquet("/home/young12/data/movie/hive/")

spark.stop()
