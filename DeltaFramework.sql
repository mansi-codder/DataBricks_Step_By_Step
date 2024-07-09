-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create your first table and grant privileges
-- MAGIC

-- COMMAND ----------

use catalog ikea;
CREATE TABLE IF NOT EXISTS default.department
(
   deptcode   INT,
   deptname  STRING,
   location  STRING
);


-- COMMAND ----------

INSERT INTO default.department VALUES
   (10, 'FINANCE', 'EDINBURGH'),
   (20, 'SOFTWARE', 'PADDINGTON');

-- COMMAND ----------

GRANT SELECT ON default.department TO `mansigandhi456@gmail.com`;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query and visualize data from a notebook

-- COMMAND ----------

SELECT * FROM samples.nyctaxi.trips LIMIT 2500


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Import and visualize CSV data from a notebook
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Step 2: Define variables

-- COMMAND ----------

CREATE SCHEMA health;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog = "ikea"
-- MAGIC schema = "health"
-- MAGIC volume = "health"
-- MAGIC download_url = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv"
-- MAGIC file_name = "baby_names.csv"
-- MAGIC file_name1 = "baby_names1.csv"
-- MAGIC table_name = "baby_names"
-- MAGIC volume = "unity-catalog/4420482296971268/Volumes/" + schema 
-- MAGIC path_table = catalog + "." + schema
-- MAGIC bucket_location ='s3://databricks-workspace-stack-ikea-bucket'
-- MAGIC path_volume1 = "/Volumes/" + catalog + "/" + schema + "/" + volume
-- MAGIC path_volume = f"{bucket_location}/{volume}"
-- MAGIC print(path_table) # Show the complete path
-- MAGIC print(path_volume) # Show the complete path
-- MAGIC print(path_volume1) # Show the complete path
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog = "ikea"
-- MAGIC schema = "health"
-- MAGIC volume = "health_volume"
-- MAGIC download_url = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv"
-- MAGIC file_name = "baby_names.csv"
-- MAGIC table_name = "baby_names"
-- MAGIC path_volume = "/Volumes/" + catalog + "/" + schema + "/" + volume
-- MAGIC path_table = catalog + "." + schema
-- MAGIC print(path_table) # Show the complete path
-- MAGIC print(path_volume) # Show the complete path

-- COMMAND ----------

-- MAGIC %python
-- MAGIC CREATE EXTERNAL VOLUME ikea.health.my_external_volume_health

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp(f"{download_url}", f"{path_volume}" + "/" + f"{file_name1}")

-- COMMAND ----------

CREATE EXTERNAL VOLUME ikea.health.my_external_volume_health
    LOCATION 's3://databricks-workspace-stack-ikea-bucket/unity-catalog/4420482296971268/Volumes/health'
    COMMENT 'This is my example external volume on S3'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 3: Import CSV file

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp(f"{download_url}", f"{path_volume}" + "/" + f"{file_name}")
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Load CSV data into a DataFrame

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC df = spark.read.csv(f"{path_volume}/{file_name}",
-- MAGIC   header=True,
-- MAGIC   inferSchema=True,
-- MAGIC   sep=",")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Save the DataFrame to a table
-- MAGIC   use withColumnRenamed

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df= df.withColumnRenamed("First Name", "First_Name")
-- MAGIC df.printSchema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Save the DataFrame to a table
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").saveAsTable(f"{path_table}" + "." + f"{table_name}")
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Add new CSV file of data to your Unity Catalog volume
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data = [[2022, "CARL", "Albany", "M", 42],[2022, "CARL", "Albany", "M", 42]]
-- MAGIC df = spark.createDataFrame(data, schema="Year int, First_Name STRING, County STRING, Sex STRING, Count int")
-- MAGIC df.coalesce(1)
-- MAGIC # display(df)
-- MAGIC # display(df.coalesce(1)
-- MAGIC # )
-- MAGIC (df.coalesce(1).
-- MAGIC     write.
-- MAGIC     option("header", "true").
-- MAGIC     mode("overwrite").
-- MAGIC     csv(f"{path_volume}/{file_name}"))
-- MAGIC
-- MAGIC  

-- COMMAND ----------

   df = spark.read.csv(f"{path_volume}/{file_name}",
  header=True,
  inferSchema=True,
  sep=",")
display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # df.write.mode("append").insertInto(f"{path_table}.{table_name}")
-- MAGIC display(spark.sql(f"SELECT * FROM {path_table}.{table_name} WHERE Year = 2022"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Ingest additional data notebooks

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data = [[2025, "CARL", "Albany", "M", 42]]
-- MAGIC
-- MAGIC df1 = spark.createDataFrame(data, schema="Year int, First_Name STRING, County STRING, Sex STRING, Count int")
-- MAGIC # display(df)
-- MAGIC (df1.coalesce(1)
-- MAGIC    .write
-- MAGIC    .option("header", "true")
-- MAGIC    .mode("overwrite")
-- MAGIC    .csv(f"{path_volume}/newbaby.csv"))
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1 = spark.read.csv(f"{path_volume}/newbaby.csv",
-- MAGIC     header=True,
-- MAGIC     inferSchema=True,
-- MAGIC     sep=",")
-- MAGIC display(df1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1.write.mode("append").insertInto(f"{path_table}.{table_name}")
-- MAGIC display(spark.sql(f"SELECT * FROM {path_table}.{table_name} WHERE Year = 2022" or "Year = 2025"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Enhance and cleanse data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC table_name = "baby_names"
-- MAGIC silver_table_name = "baby_names_prepared"
-- MAGIC gold_table_name = "top_baby_names_2021"
-- MAGIC path_table = catalog + "." + schema
-- MAGIC print(path_table) # Show the complete path

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC df_raw= spark.read.table(f"{path_table}.{table_name}")
-- MAGIC display(df_raw)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Cleanse and enhance raw data and save 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.functions import when, col, initcap
-- MAGIC
-- MAGIC df_rename_year = df_raw.withColumnRenamed("year", "year_of_birth")
-- MAGIC df_init_caps = df_rename_year.withColumn("First_Name", initcap(col("First_Name")).cast("string"))
-- MAGIC                                          
-- MAGIC df_baby_names_sex = df_init_caps.withColumn("Sex" ,
-- MAGIC                                             when(col("Sex") == "M", "Male").otherwise("Female"))
-- MAGIC display(df_baby_names_sex)
-- MAGIC
-- MAGIC df_baby_names_sex.write.mode("overwrite").saveAsTable(f"{path_table}.{silver_table_name}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Group and visualize data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import expr, sum, desc
-- MAGIC from pyspark.sql import Window
-- MAGIC
-- MAGIC # Count of names for entire state of New York by sex
-- MAGIC df_baby_names_2021_grouped=(df_baby_names_sex
-- MAGIC .filter(expr("Year_Of_Birth == 2021"))
-- MAGIC .groupBy("Sex", "First_Name")
-- MAGIC .agg(sum("Count").alias("Total_Count"))
-- MAGIC .sort(desc("First_Name")))
-- MAGIC
-- MAGIC # Display data
-- MAGIC display(df_baby_names_2021_grouped)
-- MAGIC
-- MAGIC # Save DataFrame to a table
-- MAGIC df_baby_names_2021_grouped.write.mode("overwrite").saveAsTable(f"{path_table}.{gold_table_name}")
-- MAGIC

-- COMMAND ----------


