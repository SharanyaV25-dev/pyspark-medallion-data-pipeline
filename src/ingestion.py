from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

def run_bronze() :
    spark = SparkSession.builder\
    .appName("Smartphone Usage Pipeline")\
    .getOrCreate()
    # Read the CSV file into a DataFrame
    df = spark.read.csv(
        "data/Smartphone_Usage_And_Addiction_Data.csv", 
        header=True, inferSchema=True)
    
    bronze_df = df.withColumn(
        "ingestion_timestamp",current_timestamp())\
         .withColumn(
        "source_file",
        input_file_name()
    )
    bronze_df.write.mode("overwrite").parquet("output/bronze")
    print("Bronze layer created successfully")
    spark.stop()
if __name__ == "__main__":
    run_bronze()