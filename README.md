# 🚀 Azure Retail Lakehouse Project using Azure Databricks & Azure Data Factory


# 📌 Project Overview

This project demonstrates an end-to-end Azure Lakehouse implementation using the Medallion Architecture (Bronze, Silver, Gold).

The solution ingests raw retail data into Azure Data Lake Storage using Azure Data Factory, transforms it using Azure Databricks and Delta Lake, and builds curated analytical datasets for reporting and analytics.

The project follows real-world Data Engineering practices including data ingestion, cleansing, transformation, Delta Lake implementation, Unity Catalog, and dimensional modeling.

---

# 🏗 Architecture

```
Retail Dataset
      │
      ▼
Azure Data Factory
(Ingestion Pipeline)
      │
      ▼
Azure Data Lake Storage Gen2
Bronze Layer
(Raw CSV Files)
      │
      ▼
Azure Databricks
Bronze Notebook
      │
      ▼
Silver Layer
(Cleaned Delta Tables)
      │
      ▼
Gold Layer
(Dimension & Fact Tables)
      │
      ▼
Analytics Ready Data
```

---

# 🛠 Technologies Used

- Microsoft Azure
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Apache Spark
- PySpark
- Delta Lake
- Unity Catalog
- SQL
- Medallion Architecture

---

# 📂 Project Structure

```
Azure-Retail-Lakehouse-Project

│
├── Dataset/
│
├── Notebooks/
│ ├── 01_Bronze_Data_Ingestion.ipynb
│ ├── 02_Silver_Layer.ipynb
│ └── 03_Gold_Layer.ipynb
│
├── Architecture Diagram
│
├── requirements.txt
│
├── .gitignore
│
└── README.md
```

---

# 📊 Dataset

Dataset Used

Brazilian E-Commerce Public Dataset by Olist

Tables Used

- customers
- orders
- order_items
- products
- sellers
- payments
- reviews

---

# ⚙ Project Workflow

## Step 1

Azure Data Factory Pipeline created

✔ Linked Service

✔ Dataset

✔ Copy Activity

✔ Trigger

✔ Pipeline Publish

---

## Step 2

Raw CSV files landed into Bronze Layer

Stored inside Azure Data Lake Storage Gen2.

---

## Step 3

Azure Databricks Bronze Notebook

- Read CSV
- Validate Schema
- Load Data

---

## Step 4

Silver Layer

Performed

- Data Cleaning
- Null Handling
- Data Type Conversion
- Duplicate Removal
- Delta Table Creation

---

## Step 5

Gold Layer

Created Analytical Tables

Dimension Tables

- Dim Customers
- Dim Products
- Dim Sellers

Fact Table

- Fact Sales

---

# 🗄 Medallion Architecture

Bronze Layer

Raw Data

↓

Silver Layer

Cleaned Delta Tables

↓

Gold Layer

Business Ready Analytical Tables

---

# 🚀 Features

✔ Azure Data Factory Pipeline

✔ Automated Data Ingestion

✔ Azure Data Lake Storage

✔ Delta Lake Tables

✔ Unity Catalog

✔ Medallion Architecture

✔ PySpark Transformations

✔ SQL Queries

✔ Dimension Modeling

✔ Fact Table Creation

---

# 📈 Skills Demonstrated

- Azure Data Factory
- Azure Databricks
- Azure Storage
- PySpark
- SQL
- Delta Lake
- Data Engineering
- ETL Pipeline
- Data Modeling
- Lakehouse Architecture

---

# 📌 Future Improvements

- Incremental Load
- Change Data Capture (CDC)
- Delta Merge
- Synapse Analytics Integration
- Power BI Dashboard

---

# 👩 Author

**Pooja Nandanwar**

Aspiring Data Engineer

Focused on

Azure • Databricks • PySpark • SQL • Delta Lake • Data Engineering

---

⭐ If you found this project useful, don't forget to Star this repository.
