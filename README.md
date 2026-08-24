# 🚀 Data Engineering End-to-End: Medallion Architecture on Azure

![Azure](https://img.shields.io/badge/Azure-0089D6?style=for-the-badge&logo=microsoft-azure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Data Factory](https://img.shields.io/badge/Data_Factory-32AEEB?style=for-the-badge&logo=microsoft-azure&logoColor=white)
![Synapse](https://img.shields.io/badge/Synapse_Analytics-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)

## 📌 Project Overview
This project demonstrates the implementation of a modern cloud data pipeline using the **Microsoft Azure** ecosystem. The main goal is to extract data from [Briefly mention the source, e.g., a public API or website], process it at scale, and make it business-ready using the **Medallion Architecture (Bronze, Silver, Gold)**.

## 🏗️ Solution Architecture

<img width="1472" height="760" alt="image" src="https://github.com/user-attachments/assets/0c91b025-5a7a-4697-a22d-076b6e18a7de" />


The core services utilized in this solution include:
* **Azure Data Lake Storage Gen2 (ADLS):** Centralized, hierarchical storage for the three data layers.
* **Azure Data Factory (ADF):** Orchestration and raw data ingestion.
* **Azure Databricks:** Distributed computing environment (PySpark) for data cleaning and transformation.
* **Azure Synapse Analytics:** Serverless SQL engine for ad-hoc querying and final business aggregations.
* **Azure Active Directory (App Registration):** Secure credential management via *Service Principal*.

---

## 🔄 Pipeline Workflow

### 1. Data Ingestion (Bronze Layer 🥉)
* **Tool:** Azure Data Factory
* **Process:** An ADF pipeline was configured with a *Copy Data* activity to extract the dataset from `[Source URL]`.
* **Destination:** The data is stored in its native format (e.g., CSV/JSON) in the `bronze` container within ADLS Gen2, preserving the raw historical data without modifications.
* https://ingbarcelli.github.io/Adventure-Works/dbx/Silver_layer.html

### 2. Data Transformation (Silver Layer 🥈)
* **Tool:** Azure Databricks (PySpark)
* **Process:** Databricks securely connects to the Data Lake using a **Service Principal** (OAuth 2.0). Through PySpark notebooks, the raw data is read, cleaned (handling null values, standardizing column names, data type casting), and filtered.
* **Destination:** The processed data is written to the `silver` container in **Parquet** or **Delta** format to optimize read performance.

### 3. Data Modeling & Serving (Gold Layer 🥇)
* **Tool:** Azure Synapse Analytics (Serverless SQL Pool)
* **Process:** Using Synapse Studio, logical views were created using the `OPENROWSET()` function. This allows for querying the Silver layer's Parquet files directly from the Data Lake without needing to move the data into a dedicated relational database.
* **Destination:** Aggregated tables containing the final business logic were generated and stored in the `gold` container, ready to be connected to BI tools like Power BI.

---

### Power BI modelling
* As it can be seeing data was consumption-ready and it was used to model using the mentioned visualization tool:
<img width="1535" height="792" alt="image" src="https://github.com/user-attachments/assets/4005d05a-0954-42ac-b566-d161c6859806" />
