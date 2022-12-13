from pyspark.sql import SparkSession

spark = SparkSession.Builder().appName('hashtag').getOrCreate()

Employee = spark.createDataFrame([
        ('1', 'Joe', '70000', '1'),
        ('2', 'Henry', '80000', '2'),
        ('3', 'Sam', '60000', '2'),
        ('4', 'Max', '90000', '1')],
        ['Id', 'Name', 'Sallary','DepartmentId']
)