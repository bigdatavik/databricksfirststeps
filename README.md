# Payor Medallion Lakehouse Training Project

This project demonstrates a robust, end-to-end medallion architecture for healthcare **payor data** analytics using Databricks, Delta Lake, and PySpark/Spark SQL. The pipeline covers data ingestion, cleansing, modeling, and analytics, following the **Bronze/Silver/Gold (Medallion)** pattern.

## 🚀 Overview

The notebook and supporting scripts guide you through:
- Structuring a data lakehouse for payor claims data
- Building a layered ETL process (Bronze → Silver → Gold)
- Parameterizing locations and schemas for reusable, production-ready pipeline runs
- Running scalable analytics on healthcare claims, members, providers, diagnosis, and procedure data

## 📂 Medallion Architecture

- **Bronze Layer:** Raw, minimally processed ingested data from CSV files into Delta tables
- **Silver Layer:** Cleaned, deduplicated, type-corrected, and joined data ready for analysis
- **Gold Layer:** Curated analytics tables (e.g., claims enrichment, member claim summaries)

This modular pattern ensures data lineage, scalability, and easy extensibility for additional healthcare analytics use cases.

## 🏗️ Features

- **PySpark + Spark SQL:** Hybrid approach for transformation logic, enabling easy customization and automation
- **Completely Parameterized:** All paths, database, and table names are provided as widgets/variables for effortless re-use
- **Synthetic Demo Data:** Simulated payor, claims, diagnostic, procedure, member, and provider tables for training and testing
- **Production-Grade Practices:** Explicit schema definitions, robust error handling, clear layering, and portability

## 📝 How to Use

1. **Clone or import this notebook/project to your Databricks workspace**
2. Ensure you have access to Unity Catalog volumes
3. Upload the provided sample CSV files into your configured volume locations (per parameter values)
4. (Optional) Edit the default widget parameters at the top for your target catalog, schema, and data paths
5. Run the notebook top-to-bottom  
   - Bronze: Ingest CSVs to raw Delta tables using `COPY INTO` and ETL
   - Silver: Transform and clean raw tables into analytics-ready models
   - Gold: Build enrichment and summary tables for reporting

## 📋 Data Model

**Tables included:**
- `claims_raw`, `diagnosis_raw`, `procedures_raw`, `providers_raw`, `members_raw` (Bronze)
- Silver & Gold layers build on top, implementing best practices for date/double casting, data cleaning, deduplication, and joins.

## 💡 Example Use Cases

- Healthcare claims analytics and visualization
- Payor data quality and pipeline testing
- Data platform engineering training (Databricks focused)
- Accelerating migration to medallion/lakehouse in real-world payer environments

## ⚙️ Prerequisites

- Databricks workspace (with permissions for Unity Catalog or DBFS)
- Databricks Runtime with Delta Lake support
- Upload access for CSV source files

## 🛠️ Getting Started

```bash
git clone https://github.com//.git
# or import the Databricks notebook directly via UI
```
In Databricks:

1. Open the notebook in your Workspace or Repo folder.
2. Edit the top parameter cells for your environment (optional).
3. Upload your sample data files to `/Volumes////...` as needed.
4. Run the notebook and explore your new lakehouse!

## 📑 Project Structure

```
.
├── payor_medallion_etl_notebook.py
├── data/
│   ├── claims.csv
│   ├── diagnosis.csv
│   ├── procedures.csv
│   ├── providers.csv
│   └── members.csv
├── README.md
└── LICENSE
```

## 🧑💻 Contributing

Pull requests and discussions are welcome! For bug reports or suggestions, please open a GitHub issue.

## 📚 Resources

- [Databricks Medallion Lakehouse Architecture](https://www.databricks.com/solutions/data-lakehouse)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-getting-started.html)

### © 2024  | For demonstration, education, and payor analytics development only.

**Tips:**
- Replace `/` with your GitHub namespace.
- Replace data file paths if you change folder structure.
- Add diagrams or screenshots if desired for extra clarity!

Let me know if you'd like a shorter version, alternate formatting, or content specifically tailored to data science, engineering, or curriculum needs.

[1] https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/11782157/1d551b3e-e0e5-4042-9621-894c605a06a7/python-USETHIS_payor_medallion_Load_copy_into_SQL.html
