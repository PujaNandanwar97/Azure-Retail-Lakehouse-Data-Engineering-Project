# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer

# COMMAND ----------

storage_account_name = "retaillakehouseadls01"
stroage_account_key ="storage_account_key"
stroage_account_key ="storage_account_key"
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    stroage_account_key
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Bronze Customers Table

# COMMAND ----------

df_customers = spark.table(
    "adls_retail_lakehouse.bronze.customers"
)

# COMMAND ----------

print(df_customers.count())
display(df_customers)

# COMMAND ----------

from pyspark.sql.functions import col, sum
display(
    df_customers.select(
        [
            sum(col(c).isNull().cast("int")).alias(c)
            for c in df_customers.columns
        ]
    )
)

# COMMAND ----------

total_rows = df_customers.count()

distinct_rows = df_customers.distinct().count()

print("Total Rows:", total_rows)
print("Distinct Rows:", distinct_rows)
print("Duplicates Rows:", total_rows - distinct_rows)

# COMMAND ----------

from pyspark.sql.functions import upper

df_customers_silver = df_customers.withColumn(
    "customer_city",
    upper("customer_city")
)

# COMMAND ----------

display(df_customers_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Silver Customers Table 

# COMMAND ----------

df_customers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adls_retail_lakehouse.silver.customers"
    )        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Bronze Order Table

# COMMAND ----------

df_order = spark.table(
    "adls_retail_lakehouse.bronze.orders"
)

# COMMAND ----------

df_order.printSchema()

# COMMAND ----------

display(df_order.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Real Business Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delivery Status Analysis

# COMMAND ----------

from pyspark.sql.functions import datediff
df_order_silver = df_order.withColumn(
    "delivery_days",
    datediff(
        "order_delivered_customer_date",
        "order_purchase_timestamp"
    )
)


# COMMAND ----------

display(
    df_order_silver.select(
        "order_id",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "delivery_days"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Delivery Status Column

# COMMAND ----------

from pyspark.sql.functions import when

df_order_silver = df_order_silver.withColumn(
    "delivery_status",
    when(
        df_order_silver.order_delivered_customer_date <=
        df_order_silver.order_estimated_delivery_date,
        "On Time"
    ).otherwise("Late")
)

# COMMAND ----------

display(
    df_order_silver.select(
        "order_id",
        "order_estimated_delivery_date",
        "order_delivered_customer_date",
        "delivery_status"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Order Status Validation

# COMMAND ----------

display(
    df_order.groupBy("order_status")
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Silver Orders Table

# COMMAND ----------

df_order_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adls_retail_lakehouse.silver.orders"
    )        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Bronze Order Items Table

# COMMAND ----------

df_order_items = spark.table(
    "adls_retail_lakehouse.bronze.order_items"
)

# COMMAND ----------

df_order_items.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, sum

display(
    df_order_items.select(
        [
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df_order_items.columns
        ]
    )
)

# COMMAND ----------

total_rows = df_order_items.count()

distinct_rows = df_order_items.distinct().count()
print("Total Rows :", total_rows)
print("Distinct Rows :", distinct_rows)
print("Duplicate_Rows :", total_rows - distinct_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Real Business Columns

# COMMAND ----------

from pyspark.sql.functions import col

df_order_items_silver = df_order_items.withColumn(
    "total_item_value",
    col("price") + col("freight_value")
)

# COMMAND ----------

display(
    df_order_items_silver.select(
        "order_id",
        "price",
        "freight_value",
        "total_item_value"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Order Items Silver Table

# COMMAND ----------

df_order_items_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adls_retail_lakehouse.silver.order_items"
    )        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Product Table Read

# COMMAND ----------

df_products = spark.table(
    "adls_retail_lakehouse.bronze.products"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema, Null, Duplicate Checking

# COMMAND ----------

df_products.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, sum

display(
    df_products.select(
        [
            sum(col(c).isNull().cast("int")).alias(c)
            for c in df_products.columns
        ]
    )

)

# COMMAND ----------

total_rows = df_products.count()

distinct_rows = df_products.distinct().count()
print("Total Rows :", total_rows)
print("Distinct Rows :", distinct_rows)
print("Duplicate_Rows :", total_rows - distinct_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Category Translation Table

# COMMAND ----------

df_category = spark.table(
    "adls_retail_lakehouse.bronze.product_category"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Check

# COMMAND ----------

df_category.printSchema()

# COMMAND ----------

display(df_category)

# COMMAND ----------

df_products_silver = df_products.join(
    df_category,
    on="product_category_name",
    how="left"
)

# COMMAND ----------

display(
    df_products_silver.select(
        "product_id",
        "product_category_name",
        "product_category_name_english"
    )

)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Products Silver Table

# COMMAND ----------

df_products_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adls_retail_lakehouse.silver.products"
    )        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Bronze Payment Table

# COMMAND ----------

df_payments = spark.table(
    "adls_retail_lakehouse.bronze.payments"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema, Null, Duplicate Checking

# COMMAND ----------

df_payments.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, sum
display(
    df_payments.select(
        [
            sum(col(c).isNull().cast("int")).alias(c)
            for c in df_payments.columns
        ]   
        
    )
)

# COMMAND ----------

total_rows = df_payments.count()
distinct_rows = df_payments.distinct().count()

print("Total Rows :", total_rows)
print("Distinct Rows :", distinct_rows)
print("Duplicate Rows :", total_rows - distinct_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Payment Type Validation

# COMMAND ----------

display(
    df_payments.groupBy("payment_type").count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installment Analysis

# COMMAND ----------

display(
    df_payments.groupBy("payment_installments")
    .count()
    .orderBy("payment_installments")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Silver Payment Table 

# COMMAND ----------

df_payments.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adls_retail_lakehouse.silver.payments"
    )        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Reviews Table Read

# COMMAND ----------

df_reviews_fresh =spark.table("adls_retail_lakehouse.bronze.reviews")

# COMMAND ----------

display(df_reviews_fresh.limit(5))

# COMMAND ----------

# DBTITLE 1,Cell 67
from pyspark.sql.functions import col, expr

df_reviews_fresh = df_reviews_fresh.withColumn(
    "review_score",
    expr("try_cast(review_score as int)")
)

# COMMAND ----------

display(df_reviews_fresh.select("review_score").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Sentiment Reviews Table

# COMMAND ----------

from pyspark.sql.functions import when

df_reviews_fresh = (
    df_reviews_fresh
    .withColumn(
        "review_sentiment",
        when(col("review_score") >= 4, "Positive")
        .when(col("review_score") == 3, "Neutral")
        .otherwise("Negative")
    )
)



# COMMAND ----------

display(
    df_reviews_fresh.select("review_score", "review_sentiment").limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Silver Reviews Table

# COMMAND ----------

df_reviews_fresh.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adls_retail_lakehouse.silver.reviews"
    )

# COMMAND ----------

display(
    spark.table("adls_retail_lakehouse.silver.reviews").limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Sellers Table Read

# COMMAND ----------

df_sellers = spark.table(
    "adls_retail_lakehouse.bronze.sellers"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema, Null, Duplicates Check

# COMMAND ----------

df_sellers.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, sum

display(
    df_sellers.select(
        [
            sum(col(c).isNull().cast("int")).alias(c)
            for c in df_sellers.columns
        ]
    )
)

# COMMAND ----------

total_rows = df_sellers.count()
distinct_rows = df_sellers.distinct().count()

print("Total Rows:", total_rows)
print("Distinct Rows:", distinct_rows)
print("Duplicates:", total_rows - distinct_rows)

# COMMAND ----------

df_sellers.count()

# COMMAND ----------

df_sellers_silver = df_sellers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Silver Sellers Tables

# COMMAND ----------

df_sellers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adls_retail_lakehouse.silver.sellers"
    )        

# COMMAND ----------

display(
    spark.table(
        "adls_retail_lakehouse.silver.sellers").limit(10)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Geolocation Read

# COMMAND ----------

df_geo = spark.table("adls_retail_lakehouse.bronze.geolocation")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema, Null, Duplicates Check

# COMMAND ----------

df_geo.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, sum

display(
    df_geo.select(
        [
            sum(col(c).isNull().cast("int")).alias(c)
            for c in df_geo.columns
        ]
    )
)

# COMMAND ----------

total_rows = df_geo.count()
distinct_rows = df_geo.distinct().count()

print("Total Rows:", total_rows)
print("Distinct Rows:", distinct_rows)
print("Duplicates:", total_rows - distinct_rows)

# COMMAND ----------

df_geo.dropDuplicates().count()

# COMMAND ----------

df_geo.count()

# COMMAND ----------

df_geo_silver = df_geo.dropDuplicates()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Silver Geolocation Table

# COMMAND ----------

df_geo_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "adls_retail_lakehouse.silver.geolocation"
        )

# COMMAND ----------

display(
    spark.table("adls_retail_lakehouse.silver.geolocation").limit(5)
)

# COMMAND ----------

display(df.limit(5))