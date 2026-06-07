from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, explode, split, avg, count, desc,
    row_number
)
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("DoubanSparkSQLAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.read
    .option("header", "true")
    .option("multiLine", "true")
    .option("escape", "\"")
    .csv("/opt/spark/work/douban_movies.csv")
)

for c in df.columns:
    df = df.withColumn(
        c,
        when(col(c).isNull() | (trim(col(c)) == ""), None).otherwise(col(c))
    )

movies = (
    df
    .withColumn("year_num", col("year").cast(IntegerType()))
    .withColumn("rating_score_num", col("rating_score").cast(DoubleType()))
    .withColumn("rating_count_num", col("rating_count").cast(IntegerType()))
    .withColumn("collect_count_num", col("collect_count").cast(IntegerType()))
    .filter(col("title").isNotNull())
    .filter(col("rating_score_num").isNotNull())
)

movies.createOrReplaceTempView("movies")

print("\n===== A-2 Q1：GROUP BY 聚合：不同电影类型的数量与平均评分 Top 10 =====")
genre_df = (
    movies
    .filter(col("genres").isNotNull())
    .withColumn("genre", explode(split(col("genres"), "/")))
    .groupBy("genre")
    .agg(
        count("*").alias("movie_count"),
        avg("rating_score_num").alias("avg_rating"),
        avg("collect_count_num").alias("avg_collect_count")
    )
    .orderBy(desc("movie_count"))
)
genre_df.show(10, truncate=False)

print("\n===== A-2 Q1 分析说明 =====")
print("该查询先将 genres 字段按斜杠拆分，再使用 GROUP BY 统计不同电影类型的影片数量、平均评分和平均收藏数。从结果可以观察样本中哪些类型电影数量最多，也可以比较不同类型的平均评分差异。若某类电影数量较多且平均评分较高，说明该类型既有较高覆盖度，也具有较好的用户评价基础。")

print("\n===== A-2 Q2：ORDER BY Top-N：评分人数最多的 Top 10 电影 =====")
top_rating_count_df = (
    movies
    .select("movie_id", "title", "year_num", "rating_score_num", "rating_count_num", "collect_count_num")
    .filter(col("rating_count_num").isNotNull())
    .orderBy(desc("rating_count_num"))
)
top_rating_count_df.show(10, truncate=False)

print("\n===== A-2 Q2 分析说明 =====")
print("该查询按照 rating_count_num 降序排列，选出评分人数最多的 Top 10 电影。评分人数反映了影片的观看热度和大众参与评价程度，相比单纯评分分数，评分人数更能体现影片的传播范围。Top-N 结果通常包含知名度较高、受众较广的经典影片或热门影片。")

print("\n===== A-2 Q3：时间维度趋势分析：按年份统计电影数量、平均评分与平均收藏数 =====")
year_trend_df = (
    movies
    .filter(col("year_num").isNotNull())
    .filter((col("year_num") >= 1980) & (col("year_num") <= 2025))
    .groupBy("year_num")
    .agg(
        count("*").alias("movie_count"),
        avg("rating_score_num").alias("avg_rating"),
        avg("collect_count_num").alias("avg_collect_count")
    )
    .orderBy(desc("year_num"))
)
year_trend_df.show(30, truncate=False)

print("\n===== A-2 Q3 分析说明 =====")
print("该查询以年份作为时间维度，对每年的电影数量、平均评分和平均收藏数进行统计。通过年份趋势可以观察不同时期电影样本规模和评分变化情况。若某些年份电影数量明显增加，可能与数据采集范围、电影产业产量或平台收录规模有关；平均评分和收藏数则可辅助判断不同年份影片的整体口碑与热度。")

print("\n===== A-2 Q4：窗口函数：每个国家评分最高的 Top 3 电影 =====")
country_movie_df = (
    movies
    .filter(col("countries").isNotNull())
    .withColumn("country", explode(split(col("countries"), "/")))
    .filter(col("rating_count_num").isNotNull())
    .filter(col("rating_count_num") >= 1000)
)

window_spec = Window.partitionBy("country").orderBy(desc("rating_score_num"), desc("rating_count_num"))

country_top3_df = (
    country_movie_df
    .withColumn("rank_in_country", row_number().over(window_spec))
    .filter(col("rank_in_country") <= 3)
    .select(
        "country",
        "rank_in_country",
        "title",
        "year_num",
        "rating_score_num",
        "rating_count_num"
    )
    .orderBy("country", "rank_in_country")
)
country_top3_df.show(60, truncate=False)

print("\n===== A-2 Q4 分析说明 =====")
print("该查询使用窗口函数 row_number，并按国家进行 partition，在每个国家内部按照评分和评分人数排序，筛选出各国家评分最高的 Top 3 电影。窗口函数适合解决分组内排名问题，相比普通 GROUP BY，它不仅能保留电影标题、年份等明细字段，还能得到每个国家内部的局部排名结果。")

spark.stop()
