from pathlib import Path
from shutil import copyfile

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, desc, lag, round as spark_round, row_number, to_date
from pyspark.sql.window import Window


def save_single_csv(result_dir, file_name):
    output = Path(file_name)
    if output.exists():
        output.unlink()

    part_file = next(Path(result_dir).glob("part-*.csv"))
    copyfile(part_file, output)


spark = (
    SparkSession.builder
    .appName("CovidDataFrameAnalysis")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

covid = (
    spark.read.csv("covid-data.csv", header=True, inferSchema=True)
    .withColumn("date", to_date(col("date")))
    .where(col("continent").isNotNull())
)

march_31 = to_date(col("date")) == "2021-03-31"
last_week_march = (col("date") >= "2021-03-25") & (col("date") <= "2021-03-31")

top_15_cases_percent = (
    covid
    .where(march_31)
    .where(col("population").isNotNull() & (col("population") > 0))
    .where(col("total_cases").isNotNull())
    .select(
        col("iso_code"),
        col("location").alias("country"),
        spark_round((col("total_cases") / col("population")) * 100, 2).alias("cases_percent"),
    )
    .orderBy(desc("cases_percent"), col("country"))
    .limit(15)
)

country_new_cases_window = Window.partitionBy(col("location")).orderBy(desc("new_cases"), col("date"))

top_10_new_cases = (
    covid
    .where(last_week_march)
    .where(col("new_cases").isNotNull())
    .withColumn("row_number", row_number().over(country_new_cases_window))
    .where(col("row_number") == 1)
    .select(
        date_format(col("date"), "yyyy-MM-dd").alias("date"),
        col("location").alias("country"),
        col("new_cases").cast("long").alias("new_cases"),
    )
    .orderBy(desc("new_cases"), col("country"), col("date"))
    .limit(10)
)

russia_window = Window.orderBy(col("date"))

russia_cases_delta = (
    covid
    .where(col("location") == "Russia")
    .where((col("date") >= "2021-03-24") & (col("date") <= "2021-03-31"))
    .where(col("new_cases").isNotNull())
    .select(
        col("date"),
        col("new_cases").cast("long").alias("new_cases_today"),
    )
    .withColumn("new_cases_yesterday", lag(col("new_cases_today")).over(russia_window))
    .where(col("date") >= "2021-03-25")
    .select(
        date_format(col("date"), "yyyy-MM-dd").alias("date"),
        col("new_cases_yesterday"),
        col("new_cases_today"),
        (col("new_cases_today") - col("new_cases_yesterday")).alias("delta"),
    )
    .orderBy(col("date"))
)

print("Top 15 countries by total cases percent on 2021-03-31")
top_15_cases_percent.show(15, truncate=False)

print("Top 10 countries by maximum new cases during the last week of March 2021")
top_10_new_cases.show(10, truncate=False)

print("Russia new cases delta during the last week of March 2021")
russia_cases_delta.show(10, truncate=False)

top_15_cases_percent.coalesce(1).write.mode("overwrite").option("header", True).csv("result_top_15_cases_percent")
top_10_new_cases.coalesce(1).write.mode("overwrite").option("header", True).csv("result_top_10_new_cases")
russia_cases_delta.coalesce(1).write.mode("overwrite").option("header", True).csv("result_russia_cases_delta")

save_single_csv("result_top_15_cases_percent", "top_15_cases_percent.csv")
save_single_csv("result_top_10_new_cases", "top_10_new_cases.csv")
save_single_csv("result_russia_cases_delta", "russia_cases_delta.csv")

spark.stop()
