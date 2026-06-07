from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, coalesce
from pyspark.sql.types import IntegerType, DoubleType

spark = SparkSession.builder.appName("DoubanDataCleaning").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.read
    .option("header", "true")
    .option("multiLine", "true")
    .option("escape", "\"")
    .csv("/opt/spark/work/douban_movies.csv")
)

print("\n===== 20. 加载数据到 DataFrame：Schema =====")
df.printSchema()

print("\n===== 20. 加载数据到 DataFrame：前 5 行 =====")
df.show(5, truncate=60)

total_rows = df.count()

print("\n===== 20. 各字段缺失值数量与缺失比例 =====")
missing_rows = []
for c in df.columns:
    missing_count = df.filter(col(c).isNull() | (trim(col(c)) == "")).count()
    missing_ratio = round(missing_count / total_rows * 100, 4)
    missing_rows.append((c, int(missing_count), float(missing_ratio)))

missing_df = spark.createDataFrame(
    missing_rows,
    ["field", "missing_count", "missing_ratio_percent"]
)
missing_df.show(len(df.columns), truncate=False)

print("\n===== 21. 缺失值处理策略说明 =====")
print("策略一 dropna：删除 directors 缺失的记录。原因：导演字段是电影分析的重要维度，缺失后难以可靠推断。")
print("策略二 fillna：summary 缺失填充为“暂无简介”。原因：简介属于文本描述字段，缺失时可用固定文本占位。")
print("策略三 fillna/coalesce：original_title 缺失时使用 title 填充。原因：原名缺失时可用中文片名作为替代。")
print("策略四 fillna：genres、countries 缺失填充为“未知”。原因：类别型字段缺失时用未知类别保留样本。")

print("\n===== 21. 清洗前有缺失值字段 =====")
missing_df.filter(col("missing_count") > 0).show(truncate=False)

clean_df = df
for c in df.columns:
    clean_df = clean_df.withColumn(
        c,
        when(col(c).isNull() | (trim(col(c)) == ""), None).otherwise(col(c))
    )

clean_df = clean_df.dropna(subset=["directors"])

clean_df = clean_df.withColumn("original_title", coalesce(col("original_title"), col("title")))

clean_df = clean_df.fillna({
    "summary": "暂无简介",
    "genres": "未知",
    "countries": "未知"
})

clean_df = (
    clean_df
    .withColumn("year_num", col("year").cast(IntegerType()))
    .withColumn("rating_score_num", col("rating_score").cast(DoubleType()))
    .withColumn("rating_count_num", col("rating_count").cast(IntegerType()))
    .withColumn("collect_count_num", col("collect_count").cast(IntegerType()))
)

clean_rows = clean_df.count()

print("\n===== 22. 清洗前后行数对比 =====")
print(f"清洗前行数 before_rows = {total_rows}")
print(f"清洗后行数 after_rows  = {clean_rows}")
print(f"删除行数 removed_rows  = {total_rows - clean_rows}")

print("\n===== 22. 各数值字段基本统计信息 mean/std/min/max =====")
clean_df.select(
    "year_num",
    "rating_score_num",
    "rating_count_num",
    "collect_count_num"
).summary("mean", "stddev", "min", "max").show(truncate=False)

print("\n===== 清洗后前 10 行样例 =====")
clean_df.select(
    "movie_id",
    "title",
    "original_title",
    "year_num",
    "rating_score_num",
    "genres",
    "countries",
    "directors"
).show(10, truncate=60)

spark.stop()
