# Databricks Workshop: HQRI Risk Adjustment & Analytics

This workshop is specifically designed for **healthcare data analysts, data engineers, and SAS users** who are new to Databricks. It provides a hands-on introduction to **Medicare risk adjustment analytics** using Databricks' medallion architecture (Bronze/Silver/Gold layers), with a strong focus on **HCC risk scoring**, **CMS encounter datamart**, **data quality**, and **performance optimization**.

## 🚀 Overview

This interactive workshop guides you through:
- 💰 **HCC Risk Score Calculations**: Calculate Medicare Advantage risk scores for CMS payment determination
- 📊 **Encounter Datamart**: Build CMS submission-ready encounter data with validations
- ⭐ **Star Ratings Analytics**: Revenue impact analysis and member stratification
- 🔍 **Data Quality Audits**: Comprehensive compliance checks for regulatory requirements
- 💻 **SAS to Databricks Migration**: Side-by-side comparisons of familiar SAS PROC steps
- ⚡ **Lazy Evaluation & Optimization**: Best practices for deterministic, production-ready pipelines
- 🎯 **SQL & PySpark**: Hands-on examples with both approaches


## 📂 Medallion Architecture

- **Bronze Layer (Raw Data):** Raw data ingestion from CSV files using `COPY INTO` - preserves original data for audit trails
- **Silver Layer (Cleaned Data):** Data cleansing, deduplication, type corrections, and standardization
- **Gold Layer (Business Analytics 🎉):** HCC risk scoring, revenue forecasting, compliance audits, and production pipelines

This modular pattern ensures data lineage, scalability, ACID compliance, and aligns with regulatory requirements for Medicare Advantage reporting.

## 🏗️ Features

### 🎯 Gold Layer Analytics Examples

* Example 1: HCC Risk Score Calculation
* Example 2: Revenue Forecast & Impact Analysis
* Example 3: HCC Distribution Analysis
* Example 4: Data Quality & Compliance Audit
* Example 5: Member Risk Stratification
* Example 6: Provider Performance on Risk Capture
* Example 7: Encounter Datamart for CMS Submission
* Example 8: Lazy Evaluation & Deterministic Execution ⚡

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


## 📋 HQRI Data Model

**Datasets (Bronze → Silver → Gold):**
- **Members**: Medicare Advantage enrollees (demographics, plan info, enrollment dates)
- **Claims**: Medical encounters with diagnosis codes for HCC mapping
- **Providers**: Healthcare providers (NPI, specialty, location)
- **Diagnoses**: ICD-10 diagnosis codes linked to claims
- **Procedures**: CPT/HCPCS procedure codes and charges

**Reference Data:**
- **HCC Reference Table**: ICD-10 to HCC category mapping with coefficients

**Gold Layer Analytics Tables Created:**
- `member_risk_scores`: Member-level HCC risk scores and projected payments
- `revenue_forecast`: Revenue projections by plan and risk tier
- `hcc_distribution`: HCC category revenue impact analysis
- `data_quality_audit`: CMS submission validation results
- `member_risk_stratification`: Risk tier segmentation for care management
- `provider_risk_capture_performance`: Provider HCC documentation performance
- `encounter_datamart_cms`: CMS-ready encounter submission table
- `enriched_claims_checkpoint`: Production pipeline checkpoint example


## 🛠️ Getting Started

### Prerequisites
- Databricks workspace (Community Edition or higher)
- Basic SQL knowledge
- Familiarity with healthcare payer data (helpful but not required)
- No prior Spark/PySpark experience needed

### Quick Start (5 minutes)

**In Databricks:**
1. Open the notebook `DBX Workshop_HQRI_11142025.ipynb` in your workspace
2. Run the setup cells to configure catalog, schemas, and load data
3. Follow along with examples sequentially:
4. Work through hands-on exercises and experiment with your own queries!


## 📑 Project Structure

```
├── DBX Workshop_HQRI_11142025.ipynb             ⭐ Training notebook
├── [Reference] Best Practices                    📚 Best practices guide
├── README.md                                      📖 This file
├── LICENSE.md                                      📖 License
└── data/
    ├── claims.csv                                 💰 Medical encounters
    ├── diagnoses.csv                             🏥 ICD-10 diagnosis codes
    ├── procedures.csv                            🔬 CPT/HCPCS procedure codes
    ├── providers.csv                             👨‍⚕️ Healthcare providers (with NPI)
    ├── member.csv                                 👥 Medicare Advantage enrollees
    └── Payor_Archive.zip                          📦 Source data archive
```

---

## 🎯 Workshop Objectives Summary

By the end of this workshop, you will be able to:

1. ✅ Build **Medallion Architecture** pipelines for Medicare risk adjustment data
2. ✅ Calculate **HCC risk scores** and project CMS payments
3. ✅ Create **CMS encounter datamart** tables with validation
4. ✅ Perform **data quality audits** for regulatory compliance
5. ✅ Build **Gold layer analytics** for revenue optimization
6. ✅ Implement **lazy evaluation** and **deterministic execution** best practices
7. ✅ Migrate **SAS workflows** to Databricks efficiently

---

### © 2025 | Healthcare Quality Reporting & Improvement (HQRI) Analytics Workshop
**Target Audience:** Healthcare data analysts, data engineers, and SAS users transitioning to Databricks  
**Difficulty Level:** Beginner to intermediate  
**Focus Areas:** Medicare Advantage, HCC risk adjustment, CMS submissions, performance optimization

*Last updated: November 14, 2025*