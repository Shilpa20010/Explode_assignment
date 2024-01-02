# Databricks notebook source
# DBTITLE 1,Json File read as df
df = spark.read.option("multiline",True).json("/FileStore/shexplode/sample_Json.json")
display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Final price column converted it into Array<int>
df_array = df.withColumn("Final price",split("Final price",",")).withColumn("Final price",col("Final price").cast("ARRAY<INT>"))
display(df_array)

# COMMAND ----------

# DBTITLE 1,Product Quantity column converted it into Array<int>
df_product = df_array.withColumn("Product Quantity",split("Product Quantity",",")).withColumn("Product Quantity",col("Product Quantity").cast("ARRAY<INT>"))
display(df_product)

# COMMAND ----------

# DBTITLE 1,Product basePrice column converted it into Array<Double>
df_prodbase = df_product.withColumn("Product basePrice",split("Product basePrice",",")).withColumn("Product basePrice",col("Product basePrice").cast("ARRAY<DOUBLE>"))
display(df_prodbase)

# COMMAND ----------

# DBTITLE 1,Pos explode Final price column.
df_explode_final = df_prodbase.select("*",posexplode("Final price").alias("final_position","final_exploded_value"))
display(df_explode_final)

# COMMAND ----------

# DBTITLE 1,Pos explode Product Quantity column.
df_explode_product = df_explode_final.select("*",posexplode("Product Quantity").alias("product_position","product_postion_value"))
display(df_explode_product)

# COMMAND ----------

# DBTITLE 1,Pos explode Product basePrice column.
df_explode_base = df_explode_product.select("*",posexplode("Product basePrice").alias("base_position","base_position_value"))
display(df_explode_base)

# COMMAND ----------


