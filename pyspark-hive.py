from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("pyspark").enableHiveSupport().getOrCreate()

df = spark.read.format("csv").option('header','true').load("s3://firstone3/mereged_file.csv")

df.show()

df.write.saveAsTable("mydb.pyhive")

spark.stop()