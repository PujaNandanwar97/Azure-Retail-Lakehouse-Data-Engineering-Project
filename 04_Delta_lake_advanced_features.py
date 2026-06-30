# Databricks notebook source
# MAGIC %md
# MAGIC ### Delta lake advanced features
# MAGIC ### Optimize

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize adls_retail_lakehouse.gold.fact_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vacuum

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM adls_retail_lakehouse.gold.fact_sales RETAIN 168 HOURS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Travel Check

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY adls_retail_lakehouse.gold.fact_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from adls_retail_lakehouse.gold.fact_sales
# MAGIC version as of 0