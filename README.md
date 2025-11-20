# Databricks Workshop: Data & Analytics
**For Business Intelligence & Data Analytics Professionals**

This workshop is specifically designed for **Business Intelligence and Data Analytics professionals** who are transitioning from **SAS to Databricks**. It provides a hands-on introduction to Databricks' **Medallion Architecture** (Bronze/Silver/Gold layers), with a strong focus on **SAS-to-SQL migration**, **business analytics**, **data quality**, and **performance optimization**.

## 🚀 Overview

This interactive workshop guides you through:
- 🔄 **SAS to Databricks Migration**: Side-by-side comparisons of SAS PROC SQL → Databricks SQL/PySpark
- 📊 **Customer Analytics**: Aggregations, segmentation, and business metrics calculation
- 💰 **Revenue Analysis**: Financial performance, trends, and forecasting
- 🔍 **Data Quality Audits**: Comprehensive validation checks for data governance
- 🏆 **Performance Metrics**: Top performers, rankings, and leaderboard analyses
- 🎯 **SQL & PySpark**: Hands-on examples with both approaches


## 📂 Medallion Architecture

- **Bronze Layer (Raw Data):** Raw data ingestion from CSV files using `COPY INTO` - preserves original data for audit trails
- **Silver Layer (Cleaned Data):** Data cleansing, deduplication, type corrections, and standardization
- **Gold Layer (Business Analytics 🎉):** Customer analytics, revenue forecasting, performance metrics, and production pipelines

This modular pattern ensures data lineage, scalability, ACID compliance, and is the industry-standard approach for organizing data across all domains (retail, finance, manufacturing, healthcare, etc.).

## 🏗️ Features

### 🎯 Hands-on Exercises
* **Exercise #1**: Asking Databricks Assistant
* **Exercise #2**: Calculating Risk Score
* **Exercise #3**: Calculating Revenue Forecast
* **Exercise #4**: Conducting HCC Distribution Analysis

### 💻 SAS to Databricks Migration
- **Side-by-side comparisons**: SAS PROC SQL → Databricks SQL/PySpark
- **Modern functions**: COLLECT_SET(), EXPLODE(), window functions
- **Performance advantages**: Distributed processing vs. single-server SAS
- **Cost benefits**: Pay-per-use vs. expensive SAS licensing
- **Migration best practices**: CTE-based queries, array operations, caching strategies

### 🛠️ Technical Features
- **Unity Catalog**: Unified governance, row/column-level security
- **Delta Lake**: ACID transactions, time travel, schema evolution
- **Predictive Optimization**: Automatic table maintenance and optimization
- **AI/BI & Genie**: Natural language queries and self-service analytics
- **Production-ready patterns**: Checkpointing, caching, deterministic execution


## 📋 Example Dataset (Healthcare Payer)

**Note**: This workshop uses a healthcare payer dataset as an example, but all concepts apply to any business domain.

**Datasets (Bronze → Silver → Gold):**
- **Members**: Customer/member demographics and attributes
- **Claims**: Transaction records with financial details
- **Providers**: Service provider/vendor information
- **Diagnoses**: Classification codes for categorization
- **Procedures**: Service details and associated costs


## 🛠️ Getting Started

### Prerequisites
- Databricks workspace (Community Edition or higher)
- Basic SQL knowledge (SAS PROC SQL experience is helpful)
- No prior Spark/PySpark experience needed
- Familiarity with business intelligence and analytics concepts

### Quick Start (5 minutes)

**In Databricks:**
1. Open the notebook `DBX Workshop_DnA_11202025.ipynb` in your workspace
2. Run the setup cells to configure catalog, schemas, and load example data
3. Follow along with examples sequentially:
   - Setup 
   - Bronze/Silver layer examples
   - Gold layer analytics examples
   - Hands-on SAS-to-Databricks exercises
4. Work through hands-on exercises and experiment with your own queries!


## 📑 Project Structure

```
├── DBX Workshop_DnA_11202025.ipynb              ⭐ Main training notebook
├── [Reference] Best Practices                    📚 Best practices guide
├── README.md                                 📖 This file
├── LICENSE.md                                    📖 License
└── data/
    ├── claims.csv                                💰 Transaction/claims records
    ├── diagnoses.csv                            🏥 Classification codes
    ├── procedures.csv                           🔬 Service/procedure details
    ├── providers.csv                            👨‍⚕️ Service providers/vendors
    ├── member.csv                               👥 Customer/member data
    └── Payor_Archive.zip                        📦 Source data archive
```

---

### © 2025 | Databricks Workshop: Data & Analytics
**Target Audience:** Business Intelligence and Data Analytics professionals transitioning from SAS to Databricks  
**Difficulty Level:** Beginner to intermediate  
**Focus Areas:** SAS migration, Medallion architecture, Gold layer analytics, production pipelines  

*Last updated: November 20, 2025*