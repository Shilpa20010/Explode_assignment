# Databricks notebook source
delta_table_path = "/FileStore/shexplode/Json_Assignment"

# COMMAND ----------

# DBTITLE 1,Read delta table
def read_delta_table(spark,delta_table_path):
    df = spark.read.format('delta').load(delta_table_path)
    return df

# COMMAND ----------

delta_df = read_delta_table(spark,delta_table_path)

# COMMAND ----------

display(delta_df)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, from_utc_timestamp, col, to_date, sum, year, month, count
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# DBTITLE 1,Normalize all date field in date format.
def extract_date_from_string(df, timestamp_col_name, output_col_name, date_format='E MMM dd HH:mm:ss z yyyy'):
    df = df.withColumn(timestamp_col_name, to_timestamp(col(timestamp_col_name), date_format))
    df = df.withColumn(output_col_name, to_date(col(timestamp_col_name)))
    return df

# COMMAND ----------

newdate_df = extract_date_from_string(delta_df,"Order_Creation_date","OrderCreate_date")

# COMMAND ----------

newdate2_df = extract_date_from_string(newdate_df,"Order_Dispatch_date","OrderDis_date")

# COMMAND ----------

Finaldate_df = extract_date_from_string(newdate2_df,"Order_Modification_date","OrderMod_date")

# COMMAND ----------

Finaldf = Finaldate_df.withColumnRenamed("product_postion_value","ProductQuantity_value")

# COMMAND ----------

display(Finaldf)

# COMMAND ----------

# DBTITLE 1,Revenue by Year-Month and Country
def calculate_revenue(df):
    df = df.withColumn('year_month', col('OrderCreate_date').cast('string').substr(1, 7))
    df = df.withColumn('total_revenue', col('final_exploded_value') * col('ProductQuantity_value'))
    result_df = df.groupBy('year_month', 'country').agg(sum('total_revenue').alias('revenue'))
    return result_df

# COMMAND ----------

totalrevenue_df = calculate_revenue(Finaldf)

# COMMAND ----------

display(totalrevenue_df)

# COMMAND ----------

# DBTITLE 1,Average order value function
def average_order_value(df):
    df = df.withColumn('total_order_value', col('final_exploded_value') * col('ProductQuantity_value'))
    order_summary = df.groupBy('Order_ID').agg(
        sum('total_order_value').alias('total_order_value_per_order'),
        count('Order_ID').alias('order_count')
    )
    average_order_value = order_summary.agg(
        (sum('total_order_value_per_order') / sum('order_count')).alias('average_order_value')
    )
    return average_order_value

# COMMAND ----------

ordervalue_df = average_order_value(Finaldf)

# COMMAND ----------

display(ordervalue_df)

# COMMAND ----------

# DBTITLE 1,Payment method function
def most_paymentmethod(df):
    payment_method_counts = df.groupBy('Payment_method').agg(count('*').alias('count'))
    most_popular_payment_method = payment_method_counts.orderBy(col('count').desc()).first()
    return most_popular_payment_method

# COMMAND ----------

Payment_methoddf = most_paymentmethod(Finaldf)

# COMMAND ----------

display(Payment_methoddf)

# COMMAND ----------

# DBTITLE 1,Customer on country level
def consumer_country(df):
    country_level = df.groupBy('country').agg(count('*').alias('count'))
    return country_level

# COMMAND ----------

Customer_countrylevel = consumer_country(Finaldf)

# COMMAND ----------

display(Customer_countrylevel)

# COMMAND ----------

# DBTITLE 1,Highly valuable customer
def highly_valuable_customers(df, date_column, customer_column, purchase_column, threshold=1000):
    df = df.withColumn("year", year(to_date(col(date_column))))
    df = df.groupBy(customer_column, "year") \
        .agg(sum(col(purchase_column)).alias("total_purchase"))
    df = df.withColumn("is_highly_valuable", col("total_purchase") >= threshold)
    return df

# COMMAND ----------

Highly_valuedf = highly_valuable_customers(Finaldf,"OrderCreate_date","Consumer_Email","final_exploded_value")

# COMMAND ----------

display(Highly_valuedf)

# COMMAND ----------


