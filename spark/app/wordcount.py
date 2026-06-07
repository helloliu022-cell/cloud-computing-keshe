from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, explode, split, lower, regexp_replace

spark = SparkSession.builder.appName("DoubanWordCount").getOrCreate()

df = spark.read.option("header", "true").option("multiLine", "true").option("escape", "\"").csv("/opt/spark/work/douban_movies.csv")

text_df = df.select(concat_ws(" ", col("title"), col("original_title"), col("summary")).alias("text"))

words = (
    text_df
    .select(explode(split(lower(regexp_replace(col("text"), r"[^0-9a-zA-Z\\u4e00-\\u9fa5]+", " ")), "\\s+")).alias("word"))
    .where(col("word") != "")
)

word_counts = words.groupBy("word").count().orderBy(col("count").desc())

print("===== Top 20 words from Douban dataset =====")
word_counts.show(20, truncate=False)

spark.stop()
