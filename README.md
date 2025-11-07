# Databricks for Actuaries: Healthcare Analytics Workshop

This workshop is specifically designed for **actuaries and analysts** familiar with SAS who are new to Databricks, PySpark, and SQL. It provides a hands-on, beginner-friendly introduction to healthcare payer data analytics using Databricks' medallion architecture (Bronze/Silver/Gold layers), with a strong focus on **actuarial use cases**, **data quality**, and **bias detection**.

## 🚀 Overview

This interactive workshop guides actuaries through:
- 📊 **Actuarial-focused analytics**: Loss ratios, claims trending, development triangles, frequency/severity
- 🔍 **Data quality checks**: Completeness, accuracy, duplicates, and validation
- ⚖️ **Bias detection**: Demographic, geographic, temporal, and provider bias analysis
- 💻 **SAS to Databricks translation**: Side-by-side comparisons of familiar SAS procedures
- 🎯 **Simple SQL approach**: Minimal PySpark, maximum hands-on exercises
- 📈 **Regulatory compliance**: Meeting ASOP and ACA anti-discrimination requirements


## 📂 Medallion Architecture (Simplified for Actuaries)

- **Bronze Layer (Quick Setup):** Raw data ingestion from CSV files using `COPY INTO` - streamlined for fast setup
- **Silver Layer (Quick Setup):** Basic cleaning and type corrections - get to analytics quickly!
- **Gold Layer (Main Focus 🎉):** Deep actuarial analytics, interactive exercises, and business insights

This modular pattern ensures data lineage, scalability, and aligns with actuarial workflow requirements.

## 🏗️ Features

### 🎯 Actuarial Analytics Exercises
- **Loss Ratio Analysis**: Calculate loss ratios by specialty, state, and plan type
- **Claims Trending**: Month-over-month and year-over-year growth analysis using window functions
- **Development Triangles**: Claims emergence patterns for reserving (IBNR indicators)
- **Frequency & Severity**: Risk segmentation and pricing metrics
- **High-Risk Member Identification**: Using percentile analysis (95th, 99th)

### 🔍 Data Quality & Compliance
- **Completeness Checks**: Identify missing critical fields
- **Accuracy Validation**: Detect negative amounts, future dates, zero-dollar claims
- **Duplicate Detection**: Find and quantify duplicate records
- **Bias Detection**: Demographic, geographic, temporal, and provider bias analysis
- **Regulatory Compliance**: ASOP and ACA anti-discrimination requirements

### 💻 SAS User-Friendly
- **Side-by-side SAS comparisons**: `PROC SQL`, `PROC MEANS`, `PROC FREQ`, `DATA` steps
- **Familiar concepts**: LAG functions, window functions, percentiles, rolling averages
- **Simple SQL focus**: Minimal PySpark, beginner-friendly syntax
- **Interactive exercises**: Hands-on learning with starter code and hints

### 🛠️ Technical Features
- **Databricks Unity Catalog**: Modern data governance and organization
- **Delta Lake**: ACID transactions, time travel, and data versioning
- **Parameterized setup**: Reusable for different environments
- **Synthetic demo data**: Realistic healthcare payer data for safe training

## 📝 How to Use This Workshop

### For Workshop Participants (Actuaries & Analysts)
1. **Import the notebook** to your Databricks workspace
2. **Part 1 (Quick Setup - 10 mins)**: Run Bronze/Silver cells to load data
   - Just execute the cells - they're simplified!
   - Creates your working datasets
3. **Part 2 (The Fun Part! 🎉 - 1.5 hours)**: Gold Layer Actuarial Analytics
   - Work through 6 interactive exercises
   - Compare SAS vs Databricks approaches
   - Build actuarial analytics tables
   - Detect data quality issues and bias
4. **Explore and experiment**: Modify queries, try your own analyses


## 📋 Healthcare Payer Data Model

**Datasets (Bronze → Silver → Gold):**
- **Members**: Policy data (demographics, plan info, enrollment dates)
- **Claims**: Incurred losses (amounts, dates, provider info)
- **Providers**: Network data (specialty, location, credentials)
- **Diagnoses**: ICD codes linked to claims
- **Procedures**: CPT/HCPCS codes and charges

**Gold Layer Analytics Tables Created:**
- `loss_ratios_by_segment`: Loss ratios by specialty and state
- `claims_trend_analysis`: Monthly trending with MoM/YoY growth
- `claims_development`: Development triangles for reserving

## 💡 Actuarial Use Cases Covered

### Pricing & Underwriting
- Loss ratio analysis by segment (specialty, geography, plan)
- Frequency and severity decomposition
- Risk segmentation and tiering
- Provider network cost analysis

### Reserving & Forecasting
- Claims development patterns (emergence analysis)
- IBNR indicators and completeness checks
- Monthly trend analysis with seasonality
- Age-to-age development factors

### Data Quality & Compliance
- Completeness validation (missing data detection)
- Accuracy checks (negative amounts, future dates)
- Duplicate claim detection
- Bias detection for regulatory compliance (ACA, ASOP)

### Risk Management
- High-risk member identification
- Provider outlier detection (potential fraud/error)
- Geographic and demographic bias analysis
- Temporal completeness monitoring


## 🛠️ Getting Started

### Quick Start (5 minutes)

**In Databricks:**
1. In your Databricks *Free Edition* workspace, create new **Git folder** and paste the following github link https://github.com/bigdatavik/databricksfirststeps.git
2. Run cells sequentially - start with Part 1 (Setup), then move to Part 2 (Analytics)
3. Work through the interactive exercises at your own pace!


## 📑 Project Structure

```
├── DBX Workshop_IPA Actuaries_10262025.ipynb    ⭐ Main actuarial workshop
├── [Dashboard] Actuarial Analytics              ⭐ Reference dashboard
├── [Reference] Best Practices                   ⭐ Reference best practices
├── data/
│   ├── claims.csv                                 💰 Medical claim submissions
│   ├── diagnoses.csv                             🏥 Diagnosis codes from claims
│   ├── procedures.csv                            🔬 Medical procedures performed
│   ├── providers.csv                             👨‍⚕️ Healthcare providers
│   ├── member.csv                                 👥 Health plan enrollees
│   └── Payor_Archive.zip
├── past labs/
│   ├── DBSQL_Workshop_ETL and Analytics_10072025.ipynb  (Original version)
│   └── ...
├── README.md                                      📖 This file
└── LICENSE.md
```

---

### © 2025 | Designed for actuaries, analysts, and healthcare data professionals
**Target Audience:** Actuaries and analysts transitioning from SAS to Databricks  
**Workshop Duration:** 2 hours (hands-on)  
**Difficulty Level:** Beginner-friendly with intermediate analytics concepts  

*Last updated: November 7, 2025*