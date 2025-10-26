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

## 🌟 What Makes This Workshop Special?

This isn't just another Databricks tutorial! It's specifically designed for actuaries who:
- ✅ **Want to transition from SAS** but need familiar examples and comparisons
- ✅ **Need to get productive quickly** without learning complex PySpark syntax
- ✅ **Care about actuarial use cases** like loss ratios, IBNR, and development triangles
- ✅ **Must meet compliance requirements** (ASOP, ACA, data quality standards)
- ✅ **Want hands-on practice** with real healthcare payer data scenarios

**Key Differentiators:**
- 📊 Exercises mirror actual actuarial work (pricing, reserving, risk management)
- 🔍 Emphasis on data quality and bias detection (critical for rate filings!)
- 💡 SAS PROC SQL → Databricks SQL translations throughout
- 🎯 90% SQL, 10% setup - get to analytics fast!
- ⚖️ Regulatory and ethical considerations built-in

## 📂 Medallion Architecture (Simplified for Actuaries)

- **Bronze Layer (Quick Setup):** Raw data ingestion from CSV files using `COPY INTO` - streamlined for fast setup
- **Silver Layer (Quick Setup):** Basic cleaning and type corrections - get to analytics quickly!
- **Gold Layer (Main Focus 🎉):** Deep actuarial analytics, interactive exercises, and business insights

**Workshop Philosophy:** Spend less time on data loading, more time on actuarial analysis!

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
2. **Part 1 (Quick Setup - 15 mins)**: Run Bronze/Silver cells to load data
   - Just execute the cells - they're simplified!
   - Creates your working datasets
3. **Part 2 (The Fun Part! 🎉 - 2-3 hours)**: Gold Layer Actuarial Analytics
   - Work through 6 interactive exercises
   - Compare SAS vs Databricks approaches
   - Build actuarial analytics tables
   - Detect data quality issues and bias
4. **Explore and experiment**: Modify queries, try your own analyses

### For Instructors
1. Review the "SAS to Databricks Quick Reference" section with participants
2. Emphasize the interactive exercises (marked with "YOUR TURN")
3. Recommended timing:
   - Setup: 15 minutes
   - Loss Ratios & Trending: 30 minutes
   - Development Triangles: 30 minutes
   - Interactive Exercises 1-4: 60 minutes
   - Data Quality (Exercise 5): 30-45 minutes
   - Bias Detection (Exercise 6): 45-60 minutes
4. Encourage participants to relate examples to their own work

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

## ⚙️ Prerequisites

### Technical Requirements
- Databricks workspace (with Unity Catalog access)
- Databricks Runtime 13.0+ with Delta Lake support
- CSV data files uploaded to Unity Catalog volumes

### Participant Background (Recommended)
- Actuarial or analytics role (pricing, reserving, underwriting, risk)
- Experience with SAS (PROC SQL, DATA steps, PROC MEANS/FREQ)
- Basic SQL knowledge helpful but not required
- No PySpark experience needed!

## 🛠️ Getting Started

### Quick Start (5 minutes)
```bash
git clone https://github.com/bigdatavik/databricksfirststeps.git
```

**In Databricks:**
1. Import `DBX Workshop_IPA Actuaries_10262025.ipynb` to your Workspace
2. Upload CSV files from `/data` folder to your Unity Catalog volume
3. Update parameters in the first cell (catalog name, schema, volume path)
4. Run cells sequentially - start with Part 1 (Setup), then move to Part 2 (Analytics)
5. Work through the interactive exercises at your own pace!

**First Time Using Databricks?** No problem! The notebook includes:
- Step-by-step instructions
- SAS comparison examples
- "YOUR TURN" exercises with hints
- Beginner-friendly SQL (no PySpark required for exercises)

## 📑 Project Structure

```
├── DBX Workshop_IPA Actuaries_10262025.ipynb    ⭐ Main actuarial workshop
├── WORKSHOP_UPDATES.md                           📝 Detailed exercise documentation
├── data/
│   ├── claims.csv                                 💰 Claims data
│   ├── diagnoses.csv                             🏥 ICD diagnosis codes
│   ├── procedures.csv                            🔬 CPT/HCPCS codes
│   ├── providers.csv                             👨‍⚕️ Provider network data
│   ├── member.csv                                 👥 Member/policy data
│   └── Payor_Archive.zip
├── past labs/
│   ├── DBSQL_Workshop_ETL and Analytics_10072025.ipynb  (Original version)
│   └── ...
├── README.md                                      📖 This file
└── LICENSE.md
```

## 🧑💻 Contributing

Pull requests and discussions are welcome! For bug reports or suggestions, please open a GitHub issue.

**Feedback from Actuaries Especially Welcome!** If you have ideas for additional exercises or actuarial use cases, we'd love to hear from you.

## 📚 Resources

### Databricks Documentation
- [Databricks Medallion Lakehouse Architecture](https://docs.databricks.com/aws/en/lakehouse/medallion)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Databricks SQL Guide](https://docs.databricks.com/sql/index.html)
- [Unity Catalog Overview](https://docs.databricks.com/data-governance/unity-catalog/index.html)

### Actuarial Standards & Compliance
- [Actuarial Standards of Practice (ASOP)](https://www.actuary.org/content/actuarial-standards-practice)
- [Affordable Care Act (ACA) Requirements](https://www.healthcare.gov/)
- Healthcare Data Quality Best Practices

### SAS to Databricks Migration
- [SQL Window Functions Guide](https://docs.databricks.com/sql/language-manual/functions/window_functions.html)
- [Common Table Expressions (CTEs)](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-qry-select-cte.html)

---

## 🎯 Learning Path

1. **Start Here**: `DBX Workshop_IPA Actuaries_10262025.ipynb`
2. **Reference Guide**: `WORKSHOP_UPDATES.md` for detailed exercise documentation
3. **Practice**: Modify queries, add your own analyses
4. **Apply**: Use these patterns with your own actuarial data

---

### © 2025 | Designed for actuaries, analysts, and healthcare data professionals
**Target Audience:** Actuaries and analysts transitioning from SAS to Databricks  
**Workshop Duration:** 3-4 hours (hands-on)  
**Difficulty Level:** Beginner-friendly with intermediate analytics concepts  

*Last updated: October 26, 2025*