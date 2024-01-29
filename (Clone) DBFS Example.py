# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

print("Hello word")

# COMMAND ----------

# MAGIC %scala
# MAGIC // File location and type
# MAGIC var file_location = "/FileStore/tables/Ecommerce_Customers.csv"
# MAGIC var file_type = "csv"
# MAGIC
# MAGIC // CSV options
# MAGIC val infer_schema = "true"
# MAGIC val first_row_is_header = "true"
# MAGIC val delimiter = ","
# MAGIC
# MAGIC // The applied options are for CSV files. For other file types, these will be ignored.
# MAGIC var df = spark.read.format(file_type).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load(file_location)
# MAGIC
# MAGIC display(df)
# MAGIC //df.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC //Create a view or table
# MAGIC
# MAGIC var temp_table_name = "Ecommerce_Customers_csv"
# MAGIC
# MAGIC df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `Ecommerce_Customers_csv`

# COMMAND ----------

# MAGIC %scala
# MAGIC //# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# MAGIC //# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# MAGIC //# To do so, choose your table name and uncomment the bottom line.
# MAGIC
# MAGIC val permanent_table_name = "Ecommerce_Customers_csv"
# MAGIC
# MAGIC df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets/')

# COMMAND ----------

display(files)

# COMMAND ----------


