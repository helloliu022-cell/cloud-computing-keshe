import os
import time
import pandas as pd

CSV_PATH = "/opt/spark/work/douban_movies.csv"
MODE = os.environ.get("PERF_MODE", "spark").lower()


def print_bar_chart(results):
    print("\n===== A-3 性能对比文本柱状图 =====")
    max_time = max(v for _, v in results)
    scale = 40 / max_time if max_time > 0 else 1

    for name, seconds in results:
        bar = "#" * max(1, int(seconds * scale))
        print(f"{name:<15} | {bar:<40} | {seconds:.4f}s")


def run_pandas():
    start = time.perf_counter()

    df = pd.read_csv(CSV_PATH)

    df["genres"] = df["genres"].replace("", pd.NA)
    df["rating_score"] = pd.to_numeric(df["rating_score"], errors="coerce")
    df["collect_count"] = pd.to_numeric(df["collect_count"], errors="coerce")

    genre_df = (
        df.dropna(subset=["genres", "rating_score"])
          .assign(genre=df["genres"].str.split("/"))
          .explode("genre")
    )

    result = (
        genre_df.groupby("genre")
        .agg(
            movie_count=("movie_id", "count"),
            avg_rating=("rating_score", "mean"),
            avg_collect_count=("collect_count", "mean")
        )
        .sort_values("movie_count", ascending=False)
        .head(10)
    )

    elapsed = time.perf_counter() - start

    print("\n===== A-3 Pandas 单机 GROUP BY 聚合结果 Top 10 =====")
    print(result)

    print("\n===== A-3 Pandas 单机耗时 =====")
    print(f"PANDAS_TIME_SECONDS={elapsed:.6f}")

    return elapsed


def run_spark():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when, trim, explode, split, avg, count, desc
    from pyspark.sql.types import DoubleType

    spark = SparkSession.builder.appName("A3PerfCompareGenreGroupBy").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    executor_instances = spark.conf.get("spark.executor.instances", "unknown")

    start = time.perf_counter()

    df = (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .option("escape", "\"")
        .csv(CSV_PATH)
    )

    for c in df.columns:
        df = df.withColumn(
            c,
            when(col(c).isNull() | (trim(col(c)) == ""), None).otherwise(col(c))
        )

    movies = (
        df
        .withColumn("rating_score_num", col("rating_score").cast(DoubleType()))
        .withColumn("collect_count_num", col("collect_count").cast(DoubleType()))
        .filter(col("genres").isNotNull())
        .filter(col("rating_score_num").isNotNull())
    )

    result_df = (
        movies
        .withColumn("genre", explode(split(col("genres"), "/")))
        .groupBy("genre")
        .agg(
            count("*").alias("movie_count"),
            avg("rating_score_num").alias("avg_rating"),
            avg("collect_count_num").alias("avg_collect_count")
        )
        .orderBy(desc("movie_count"))
    )

    top10 = result_df.limit(10).collect()

    elapsed = time.perf_counter() - start

    print(f"\n===== A-3 PySpark executorInstances={executor_instances} GROUP BY 聚合结果 Top 10 =====")
    for row in top10:
        print(row)

    print(f"\n===== A-3 PySpark executorInstances={executor_instances} 耗时 =====")
    print(f"SPARK_EXECUTOR_INSTANCES={executor_instances}")
    print(f"SPARK_TIME_SECONDS={elapsed:.6f}")

    spark.stop()
    return elapsed


if __name__ == "__main__":
    if MODE == "pandas":
        pandas_time = run_pandas()
        print_bar_chart([("Pandas", pandas_time)])
    else:
        spark_time = run_spark()
        print_bar_chart([(f"PySpark-{os.environ.get('SPARK_EXECUTOR_INSTANCES', 'N')}", spark_time)])
