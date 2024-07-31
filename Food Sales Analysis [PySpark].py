# Databricks notebook source
# DBTITLE 1,Creating Sales DataFrame
# Sales
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

customerSchema = StructType([
    StructField("product_id" ,IntegerType(), True),
    StructField("customer_id" ,StringType(), True),
    StructField("order_date" ,DateType(), True),
    StructField("location" ,StringType(), True),
    StructField("order_source" ,StringType(), True),
])

sales_df = spark.read.format("csv").option("inferschema", "true").schema(customerSchema).load("/FileStore/tables/sales.csv")
display(sales_df)

# COMMAND ----------

# DBTITLE 1,Deriving Year, month, quater
from pyspark.sql.functions import month, year, quarter

sales_df = sales_df.withColumn("order_year", year(sales_df.order_date))
sales_df = sales_df.withColumn("order_month", month(sales_df.order_date))
sales_df = sales_df.withColumn("order_quarter", quarter(sales_df.order_date))

# COMMAND ----------

# DBTITLE 1,Menu DataFrame
customerSchema = StructType([
    StructField("product_id" ,IntegerType(), True),
    StructField("product_name" ,StringType(), True),
    StructField("price" , StringType(), True),
])

menu_df = spark.read.format("csv").option("inferschema", "true").schema(customerSchema).load("/FileStore/tables/menu.csv")
display(menu_df)

# COMMAND ----------

# DBTITLE 1,Total Amount spent by each customer
total_amount_spent = sales_df.join(menu_df, 'product_id').groupBy('customer_id').agg({'price': 'sum'}).orderBy('customer_id')
display(total_amount_spent)

# COMMAND ----------

# DBTITLE 1,Total Amount spent by each food category
total_amount_each_food = sales_df.join(menu_df, 'product_id').groupBy("product_name").agg({'price': 'sum'})

display(total_amount_each_food)

# COMMAND ----------

# DBTITLE 1,Total Amount of sales in each month
display(sales_df.join(menu_df, "product_id").groupBy("order_month").agg({'price':'sum'}).orderBy("order_month"))

# COMMAND ----------

# DBTITLE 1,Yearly Sales
display(sales_df.join(menu_df, "product_id").groupBy("order_year").agg({'price':'sum'}).orderBy("order_year"))

# COMMAND ----------

# DBTITLE 1,Quarterly Sales
display(sales_df.join(menu_df, "product_id").groupBy("order_quarter").agg({'price':'sum'}).orderBy("order_quarter"))

# COMMAND ----------

# DBTITLE 1,Total number of order by each category
from pyspark.sql.functions import count

display(sales_df.join(menu_df, "product_id").groupBy("product_id","product_name").agg(count('product_id').alias('product_count')).orderBy("product_count", ascending=False).drop("product_id"))

# COMMAND ----------

# DBTITLE 1,Top 5 ordered items
from pyspark.sql.functions import count

display(sales_df.join(menu_df, "product_id").groupBy("product_id","product_name").agg(count('product_id').alias('product_count')).orderBy("product_count", ascending=False).drop("product_id").limit(5))

# COMMAND ----------

# DBTITLE 1,Top Ordered Item
from pyspark.sql.functions import count

display(sales_df.join(menu_df, "product_id").groupBy("product_id","product_name").agg(count('product_id').alias('product_count')).orderBy("product_count", ascending=False).drop("product_id").limit(1))

# COMMAND ----------

# DBTITLE 1,frequency of customer visisted
from pyspark.sql.functions import countDistinct

display(sales_df.filter(sales_df.order_source=="Restaurant").groupBy('customer_id').agg(countDistinct('order_date')))

# COMMAND ----------

# DBTITLE 1,Total Sales by each country
display(sales_df.join(menu_df, "product_id").groupBy('location').agg({'price': 'sum'}))

# COMMAND ----------

# DBTITLE 1,total sales by order_source
display(sales_df.join(menu_df, "product_id").groupBy("order_source").agg({'price':'sum'}))
