from pyspark.sql import SparkSession

print ("Spark session initiated")
spark = SparkSession.builder \
    .appName("ReadParquetExample") \
    .getOrCreate()

parquet_file_path = "s3://m6-sf-stage/oracleM6/equipment/*"  # or local path like "data/my-file.parquet"
df = spark.read.parquet(parquet_file_path)
print ("Reading is completed")
df.printSchema()

df.show(5)
spark.stop()
