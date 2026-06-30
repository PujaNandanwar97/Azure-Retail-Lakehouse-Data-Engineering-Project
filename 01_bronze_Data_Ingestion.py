# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Bronze/silver/Gold Schemas

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS adls_retail_lakehouse.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS adls_retail_lakehouse.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS adls_retail_lakehouse.gold")

# COMMAND ----------

storage_account_name = "retaillakehouseadls01"
stroage_account_key ="storage_account_key"
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    stroage_account_key
)

# COMMAND ----------

df_customers = spark.read.csv(
    "abfss://bronze@retaillakehouseadls01.dfs.core.windows.net/olist/olist_customers_dataset.csv",
    header=True,
    inferSchema=True
)
display(df_customers)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect Data

# COMMAND ----------

df_customers.printSchema()

# COMMAND ----------

display(df_customers)

# COMMAND ----------

print(df_customers.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Bronze Customers Delta Table

# COMMAND ----------

df_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adls_retail_lakehouse.bronze.customers"
    )        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orders Bronze Table

# COMMAND ----------

df_orders = spark.read.csv(
    "abfss://bronze@retaillakehouseadls01.dfs.core.windows.net/olist/olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)
display(df_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect Data

# COMMAND ----------

df_orders.printSchema()

# COMMAND ----------

display(df_orders)

# COMMAND ----------

print(df_orders.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Orders Bronze Table To Delta Table

# COMMAND ----------

df_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adls_retail_lakehouse.bronze.orders"
    )        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Order Items Bronze Table

# COMMAND ----------

df_order_items_new = spark.read.csv(
    f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/olist/olist_order_items_dataset.csv",
      header=True,
      inferSchema=True
)

# COMMAND ----------

df_order_items_new.count()

# COMMAND ----------

df_order_items_new.printSchema()

# COMMAND ----------

display(df_order_items_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Order Items Bronze Table into Delta Table 

# COMMAND ----------

df_order_items_new.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adls_retail_lakehouse.bronze.order_items"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Reusable Bronze Ingestion Function

# COMMAND ----------

def load_to_bronze(file_name, table_name):

    df = spark.read.csv(
        f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/olist/{file_name}",
        header=True,
        inferSchema=True
    )
    print(f"Row Loaded: {df.count()}")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
            f"adls_retail_lakehouse.bronze.{table_name}"
        )
    print(f"Table Created: bronze{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products

# COMMAND ----------

load_to_bronze("olist_products_dataset.csv", "products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Payments

# COMMAND ----------

load_to_bronze("olist_order_payments_dataset.csv", "payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reviews

# COMMAND ----------

load_to_bronze("olist_order_reviews_dataset.csv", "reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sellers

# COMMAND ----------

load_to_bronze("olist_sellers_dataset.csv", "sellers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Category Translation

# COMMAND ----------

load_to_bronze("product_category_name_translation.csv", "product_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Geolocation

# COMMAND ----------

load_to_bronze("olist_geolocation_dataset.csv", "geolocation")