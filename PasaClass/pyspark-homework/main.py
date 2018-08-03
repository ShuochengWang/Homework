from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = SparkSession.builder.master("local").appName("pyspark homework").getOrCreate()
    file_path = "hdfs:///dataset/bank-data.csv"
    df = spark.read.csv(path=file_path, header=True, inferSchema=True)

    df.groupBy("sex").agg(F.min("income"), F.max("income"), F.mean("income")).show()

    df.groupBy("region").agg({"income": "mean"}).show()