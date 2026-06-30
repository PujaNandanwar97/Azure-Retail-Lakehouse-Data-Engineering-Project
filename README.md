🚀 Azure Retail Lakehouse Data Engineering Project

📖 Project Overview

This project demonstrates an end-to-end Azure Lakehouse Data Engineering solution built using Azure Data Lake Storage Gen2, Azure Databricks, PySpark, Delta Lake, SQL, and Unity Catalog.

The project follows the Medallion Architecture (Bronze → Silver → Gold) to transform raw retail data into analytics-ready datasets.

The implementation covers the complete ETL lifecycle—from raw data ingestion and cleansing to dimensional modeling and fact table creation—using cloud-native Azure technologies.

---

🎯 Business Problem

Retail companies generate large volumes of transactional data from customers, orders, products, sellers, payments, and reviews.

Raw operational datasets are not suitable for analytics because they contain inconsistencies, duplicates, and normalized transactional structures.

The objective of this project is to build a scalable Azure Lakehouse pipeline that transforms raw data into business-ready analytical datasets for reporting and decision-making.

---

🏗️ Solution Architecture

«Architecture Diagram»

(Replace the image below with your own architecture diagram after uploading it to the repository.)

                    Brazilian Olist Dataset
                             │
                             ▼
          Azure Data Lake Storage Gen2 (Bronze)
                             │
                             ▼
                  Azure Databricks (PySpark)
                             │
                             ▼
                  Silver Layer (Clean Data)
                             │
                             ▼
               Gold Layer (Fact & Dimensions)
                             │
                             ▼
                  Analytics Ready Data

---

🥉 Bronze Layer

Purpose

Store raw source data exactly as received.

Activities

- Raw CSV ingestion
- Schema inference
- Delta conversion
- Historical preservation

Output

Raw Delta Tables

---

🥈 Silver Layer

Purpose

Transform raw data into clean, validated, and standardized datasets.

Activities

- Data Cleaning
- Null Handling
- Duplicate Removal
- Schema Standardization
- Business Transformations

Output

Clean Delta Tables

---

🥇 Gold Layer

Purpose

Build business-ready analytical tables optimized for reporting and analytics.

Dimension Tables

- Dim Customers
- Dim Products
- Dim Sellers

Fact Table

- Fact Sales

---

⚙️ Azure Services Used

- Azure Data Lake Storage Gen2
- Azure Databricks
- Unity Catalog

---

💻 Technologies Used

- Python
- PySpark
- Apache Spark
- SQL
- Delta Lake
- Git
- GitHub

---

📂 Repository Structure

Azure-Retail-Lakehouse-Data-Engineering-Project
│
├── notebooks
│ ├── 01_Bronze_Data_Ingestion
│ ├── 02_Silver_Layer
│ ├── 03_Gold_Layer
│ └── 04_Delta_Lake_Advanced_Features
│
├── architecture
│ └── architecture.png
│
├── dataset
│ └── README.md
│
├── requirements.txt
├── .gitignore
└── README.md

---

🔄 Data Flow

CSV Files

↓

Azure Data Lake Storage Gen2

↓

Bronze Layer

↓

Silver Layer

↓

Gold Layer

↓

Business Ready Delta Tables

---

📊 Gold Layer Tables

Table| Description
Dim Customers| Customer Dimension
Dim Products| Product Dimension
Dim Sellers| Seller Dimension
Fact Sales| Sales Fact Table

---

🚀 Key Features

- End-to-End Azure Lakehouse
- Medallion Architecture
- Delta Lake Storage
- Azure Databricks
- Unity Catalog
- PySpark Transformations
- Data Cleaning Pipeline
- Star Schema Design
- Fact & Dimension Modeling
- Cloud Data Engineering

---

📚 Dataset

Dataset: Brazilian E-Commerce Public Dataset by Olist

Source:

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

---

🚧 Challenges Solved

During the development of this project, several practical cloud engineering challenges were solved:

- Configuring Azure Data Lake Storage Gen2 with Databricks
- Working with Delta Lake tables
- Managing Unity Catalog tables
- Building Bronze, Silver, and Gold layers
- Implementing dimensional modeling
- Resolving storage authentication issues
- Validating Delta outputs
- Handling managed vs external storage behavior

---

🎓 Skills Demonstrated

- Azure Data Engineering
- ETL Development
- PySpark
- Apache Spark
- Delta Lake
- Data Cleaning
- Data Transformation
- Data Modeling
- Star Schema
- Cloud Storage
- Git Version Control

---

📖 Key Learnings

This project helped strengthen practical knowledge in:

- Azure Lakehouse Architecture
- Medallion Architecture
- Delta Lake
- Unity Catalog
- ETL Pipeline Development
- PySpark Data Processing
- Azure Storage Integration
- Cloud Data Engineering Best Practices

---

▶️ How to Run

1. Upload the Olist dataset to Azure Data Lake Storage Gen2.
2. Run the Bronze notebook.
3. Run the Silver notebook.
4. Run the Gold notebook.
5. Validate Delta tables.
6. Query Gold tables using Unity Catalog.

---

🚀 Future Enhancements

- Incremental Data Loading
- Azure Data Factory Pipeline
- Azure Synapse Analytics
- CI/CD Pipeline
- Data Quality Monitoring
- Automated Pipeline Scheduling

---

👩‍💻 Author

Pooja Nandanwar

Aspiring Data Engineer

Passionate about Azure Data Engineering, PySpark, Delta Lake, ETL Pipelines, and Cloud Analytics.

---

⭐ If you found this project useful, please consider giving it a Star!
