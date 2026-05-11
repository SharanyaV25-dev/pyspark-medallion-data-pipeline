from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, count, round
import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
def run_gold() :
    spark = SparkSession.builder\
    .appName("Gold Layer Aggregation")\
    .getOrCreate()

    df =  spark.read.parquet("output/silver")
    print("Silver data loaded")

    # 1. User Summary
    user_summary = df.groupBy("user_id").agg(
        avg("daily_screen_time_hours").alias("avg_daily_usage"),
        avg("gaming_hours").alias("avg_gaming_hours"),
        sum("notifications_per_day").alias("total_notifications"),
        sum("app_opens_per_day").alias("total_app_opens")
    )
    user_summary.write.mode("overwrite").parquet(
        "output/gold/user_summary")
    print("User summary created")

    # 2. Addiction Analysis
    addiction_analysis = df.groupBy("addiction_level").agg(
        count("*").alias("user_count"),
        round(avg("daily_screen_time_hours"), 2).alias("avg_screen_time"),
        round(avg("sleep_hours"),2).alias("avg_sleep")
    )
    addiction_analysis.write.mode("overwrite").parquet(
        "output/gold/addiction_analysis")
    print("Addiction analysis created")

    # 3. Stress Analysis
    stress_analysis = df.groupBy("stress_level").agg(
        round(avg("daily_screen_time_hours"), 2).alias("avg_screen_time"),
        round(avg("sleep_hours"), 2).alias("avg_sleep")
    )
    stress_analysis.write.mode("overwrite").parquet(
        "output/gold/stress_analysis")
    print("Stress analysis created")
    spark.stop()

if __name__ == "__main__":
    run_gold()