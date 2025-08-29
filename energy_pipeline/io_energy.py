from pyspark.sql import SparkSession, DataFrame

def get_spark(app_name: str = "EnergyDataPipeline") -> SparkSession:
    return (SparkSession.builder
            .master("local[*]")
            .appName(app_name)
            .getOrCreate())

def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(path))
