from pyspark.sql import SparkSession
from pyspark.sql.functions import col,initcap,coalesce,lit
import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
def run_silver() :
    spark = SparkSession.builder\
    .appName("Silver Layer Transformation")\
    .getOrCreate()
    df =  spark.read.parquet("output/bronze")  
    print("Bronze data loaded")
    print("Rows before cleaning:", df.count())
    # Remove duplicates based on transaction_id
    df = df.dropDuplicates(["transaction_id"])
    # Fill missing values in addiction_level with "Unknown"
    df = df.fillna({"addiction_level": "Unknown"})
    df = df.withColumn("gender",initcap(col("gender")))\
           .withColumn("stress_level",initcap(col("stress_level")))\
           .withColumn("academic_work_impact",initcap(col("academic_work_impact")))
    
    print("Rows after cleaning:", df.count())
    df.write.mode("overwrite").parquet("output/silver")
    print("Silver layer created successfully")
    spark.stop()
if __name__ == "__main__":
    run_silver()