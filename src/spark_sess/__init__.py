from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("spark_sess")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.2.2,org.postgresql:postgresql:42.6.0",
    )
    .config(
        "fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    )
    .config("spark.hadoop.fs.s3a.impl.disable.cache", True)
    .config("spark.hadoop.fs.s3a.path.style.access", True)
    .config("spark.driver.memory", "10g")
    .config("spark.driver.memoryOverhead", "10g")
    .config("spark.executor.memoryOverhead", "10g")
    .getOrCreate()
)
