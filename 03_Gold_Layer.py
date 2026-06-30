# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Gold Schema

# COMMAND ----------

storage_account_name = "retaillakehouseadls01"
stroage_account_key ="storage_account_key"
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    stroage_account_key 
)

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS adls_retail_lakehouse.gold
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Dim Customers

# COMMAND ----------

df_dim_customers = spark.table(
    "adls_retail_lakehouse.silver.customers"
)

df_dim_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adls_retail_lakehouse.gold.dim_customers"
    )        

# COMMAND ----------

display(
    spark.table(
        "adls_retail_lakehouse.gold.dim_customers").limit(5)
    
)

# COMMAND ----------

df_dim_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://gold@retaillakehouseadls01.dfs.core.windows.net/olist/dim_customers"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Dim Products

# COMMAND ----------

df_dim_products = spark.table(
    "adls_retail_lakehouse.silver.products"
)

df_dim_products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adls_retail_lakehouse.gold.dim_products"
    )        


# COMMAND ----------

display(
    spark.table(
        "adls_retail_lakehouse.gold.dim_products").limit(5)
    
)

# COMMAND ----------

df_dim_products.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://gold@retaillakehouseadls01.dfs.core.windows.net/olist/dim_products"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Dim Sellers

# COMMAND ----------

df_dim_sellers = spark.table(
    "adls_retail_lakehouse.silver.sellers"
)

df_dim_sellers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adls_retail_lakehouse.gold.dim_sellers"
    )    

# COMMAND ----------

display(
    spark.table(
        "adls_retail_lakehouse.gold.dim_sellers").limit(5)
)

# COMMAND ----------

df_dim_sellers.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://gold@retaillakehouseadls01.dfs.core.windows.net/olist/dim_sellers"
    )

# COMMAND ----------

spark.sql("SHOW TABLES IN adls_retail_lakehouse.gold").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Tables Load

# COMMAND ----------

df_orders = spark.table(
    "adls_retail_lakehouse.silver.orders"
)

df_orders_items = spark.table(
    "adls_retail_lakehouse.silver.order_items"
)

df_payments = spark.table(
    "adls_retail_lakehouse.silver.payments"
)

df_reviews = spark.table(
    "adls_retail_lakehouse.silver.reviews"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Fact Sales

# COMMAND ----------

df_fact_sales = (
    df_orders_items
    .join(
        df_orders, 
        "order_id",
         "left"
)
.join(
    df_payments, 
    "order_id",
    "left"
)
.join(
    df_reviews.select(
        "order_id",
        "review_score",
        "review_sentiment"
    ),
    "order_id",
    "left"
)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Schema, Data

# COMMAND ----------

df_fact_sales.printSchema()

# COMMAND ----------

display(df_fact_sales.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Gold Fact Table

# COMMAND ----------

df_fact_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adls_retail_lakehouse.gold.fact_sales"
        )

# COMMAND ----------

df_fact_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://gold@retaillakehouseadls01.dfs.core.windows.net/olist/fact_sales"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation

# COMMAND ----------

spark.sql("""
SHOW TABLES IN adls_retail_lakehouse.gold
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact Table Row Count

# COMMAND ----------

spark.table(
    "adls_retail_lakehouse.gold.fact_sales"
).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Validation
# MAGIC Total Revenue

# COMMAND ----------

from pyspark.sql.functions import sum

spark.table(
    "adls_retail_lakehouse.gold.fact_sales"
).select(
    sum("payment_value")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Average Review Score

# COMMAND ----------

from pyspark.sql.functions import avg

spark.table(
    "adls_retail_lakehouse.gold.fact_sales"
).select(
    avg("payment_value")
).show()