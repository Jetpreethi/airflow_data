from pyspark.sql import SparkSession

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("MergeDataJob") \
    .enableHiveSupport() \
    .getOrCreate()

# Load data from AWS RDS (MySQL)
jdbc_url = "jdbc:mysql://sales1.ctq8cqeeq8hk.ap-south-1.rds.amazonaws.com:3306/sales1"  # Replace with your actual RDS endpoint and database name
rds_properties = {
    "user": "admin",        # Replace with your RDS username
    "password": "rootpassword",    # Replace with your RDS password
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Load data from the RDS 'sales_table'
sales_df = spark.read.jdbc(url=jdbc_url, table="sales_data", properties=rds_properties)

# Set the Hive database (replace 'db' with your actual Hive database name)
spark.sql("USE db")

# Load data from Hive tables
features_df = spark.sql("SELECT * FROM db.pyhive")  # Replace 'features_table' with your actual Hive table name
stores_df = spark.sql("SELECT * FROM default.cleaned_hive")      # Replace 'stores_table' with your actual Hive table name

sales_df = sales_df.withColumnRenamed("store", "sales_store") \
                   .withColumnRenamed("date", "sales_date") \
                   .withColumnRenamed("isholiday", "sales_isholiday")

features_df = features_df.withColumnRenamed("store", "features_store") \
                         .withColumnRenamed("date", "features_date") \
                         .withColumnRenamed("isholiday", "features_isholiday")

stores_df = stores_df.withColumnRenamed("store", "stores_store") \
                     .withColumnRenamed("date", "stores_date") \
                     .withColumnRenamed("isholiday", "stores_isholiday")

# Show loaded data for verification
print("Sales DataFrame from RDS:")
sales_df.show()

print("Features DataFrame from Hive:")
features_df.show()

print("Stores DataFrame from Hive:")
stores_df.show()

# Perform merge operations
# Example: Merging DataFrames using a common column (replace 'common_column' with your actual column name)
merged_df = sales_df.join(features_df,sales_df["sales_store"]== features_df["features_store"], how = "inner") .join(stores_df,sales_df["sales_store"]==stores_df["store>

# Show merged data for verification
print("Merged DataFrame:")
merged_df.show()

# Write merged data back to AWS RDS (MySQL) as a new table
# Ensure that the table name in RDS is different from the source tables
jdbc_url_target = "jdbc:mysql://mergeddata.ctq8cqeeq8hk.ap-south-1.rds.amazonaws.com:3306/mergeddata"
rds_properties_target = {
    "user": "admin",
    "password": "rootpassword",
    "driver": "com.mysql.cj.jdbc.Driver"
}
# Write merged data back to  rds
merged_df.write.jdbc(url=jdbc_url_target, table="merged_data_table", mode="overwrite", properties=rds_properties_target)  # Replace 'db.merged_data_table' with your de>

print("Data has been successfully merged and written to rds.")

# Stop the Spark session
spark.stop()

