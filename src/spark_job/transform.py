from pyspark.sql import SparkSession

def main():
    spark = SparkSession.Builder().\
        appName('transform_music_chart').getOrCreate()
    df = spark.read.format("mongo").load()
    df.write.csv('temp')