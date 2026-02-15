# Data Engineer Technical Assessment

# 🎯 Objective

This assessment simulates a real-world data engineering project.

You are required to design and implement a mini data platform that ingests, processes, transforms, and serves data for analytics and fraud detection use cases.

This evaluation is designed to test:
- Technical depth
- Engineering thinking
- Scalability awareness
- Production-readiness
- Professional Git workflow

This is not just a coding task — it is an engineering assessment.

---

# 🧠 Skills Being Evaluated

## 🐍 Python
- Data ingestion scripting
- File handling (CSV, JSON)
- Schema validation
- Error handling & logging
- Modular and reusable code
- Config-driven development

## 🗄 SQL
- Star schema design
- Fact & dimension modeling
- SCD Type 2 implementation
- Window functions
- Analytical queries
- Query optimization
- Indexing strategy

## ⚡ PySpark
- Large-scale transformations
- Deduplication strategies
- Null handling
- Efficient joins
- Partitioning strategy
- Data skew handling
- Performance optimization

## 📡 Streaming
- Structured streaming concepts
- Window aggregations
- Watermark handling
- Late-arriving data logic

## 🏗 Data Engineering Fundamentals
- Bronze / Silver / Gold architecture
- Incremental loads
- Idempotent pipelines
- Data quality validation
- Logging and monitoring
- Scalable system design

## 💼 Professional Engineering Workflow
- Git branching strategy
- Clean commit history
- Pull Request quality
- Clear documentation
- Reproducibility

---

# 📦 Dataset Overview

You are provided with:

- 50,000+ customers (including SCD changes)
- 5,000 products
- 150,000+ transactions  
  - Includes duplicates
  - Null values
  - Skewed keys
  - Fraud spikes
  - Late-arriving records
- 100,000 application logs
- Streaming batch simulation files

The data intentionally includes:
- Corrupted records
- Missing values
- Data skew
- Historical changes
- Inconsistent patterns

Your solution must handle these gracefully.

---

# 🏗 Expected Architecture

Implement a layered data platform:

RAW → BRONZE → SILVER → GOLD

## Bronze Layer
- Raw ingestion
- Schema enforcement
- Minimal cleaning
- Store as partitioned parquet

## Silver Layer
- Deduplicate records
- Handle null values
- Apply SCD Type 2 logic
- Join dimension tables
- Clean and standardize data

## Gold Layer
- Aggregated KPIs
- Fraud detection outputs
- Business-ready analytical tables

---

# 📋 Tasks To Complete

## 1️⃣ Data Ingestion (Python)
- Generate data using the generate code in `../data/raw` & ingest raw data from there
- Validate schema
- Log ingestion metrics
- Handle corrupted records
- Write cleaned data to Bronze layer

---

## 2️⃣ Data Warehouse (SQL)
- Design star schema
- Implement SCD Type 2 for customers
- Create fact table for transactions
- Write analytical queries:
  - Monthly revenue
  - Top products
  - Regional performance
  - Customer retention

---

## 3️⃣ PySpark Transformations
- Deduplicate transactions
- Handle null values
- Optimize joins
- Manage skewed data
- Partition output efficiently
- Produce Silver and Gold datasets

---

## 4️⃣ Fraud Detection
Design logic to:
- Detect abnormal transaction spikes
- Flag suspicious customers
- Store flagged records separately

---

## 5️⃣ Streaming Simulation
- Process streaming batch files
- Implement window-based aggregations
- Handle late-arriving data
- Demonstrate watermark usage

---

# 🍴 Git Workflow (MANDATORY)

You must follow this workflow.

## Step 1: Fork the Repository

Click **Fork** on the top right of this repository.

This creates a copy under your GitHub account.

## Step 2: Clone Your Fork

## Step 3: Create a Feature Branch

Do NOT work on `main`.

## Step 4: Implement Your Solution

Commit frequently with meaningful messages.

## Step 5: Push to Your Fork

## Step 6: Create a Pull Request (PR)

From your fork:

1. Click **Compare & Pull Request**
2. Submit PR to the original repository's `main` branch
3. Provide a detailed PR description

---

# 📝 Pull Request Must Include

Your PR description must clearly explain:

## Architecture Overview
- Layer design
- Partitioning strategy
- Data flow

## Data Modeling Decisions
- Fact & dimension design
- SCD implementation approach

## Performance Considerations
- Join optimization
- Skew handling
- Partitioning logic

## Assumptions
- Any assumptions about messy data

## How To Run
- Clear execution steps

PRs without documentation will not be evaluated.

---

# 🛠 Setup Instructions

## Option 1: Virtual Environment

## Option 2: Docker

# 📂 Suggested Folder Structure

project/
├── config/
├── ingestion/
├── spark_jobs/
├── warehouse/
├── streaming/
├── fraud/
├── data/
└── README.md


You may improve this structure if justified.

---

# 📊 Evaluation Criteria

You will be evaluated on:

- Correctness
- Code quality
- SQL modeling accuracy
- Spark optimization
- SCD implementation
- Scalability thinking
- Logging quality
- Error handling
- Clean Git history
- Pull Request clarity
- Documentation completeness

---

# 🚫 Rules

- Do NOT modify original raw datasets.
- Do NOT commit large parquet outputs.
- Use `.gitignore` properly.
- Avoid pushing virtual environments.
- Avoid hardcoding file paths.
- Use configuration files.
- Keep commits clean and logical.
- Do NOT work directly on main branch.

---

# ⏳ Timeline

Expected completion time: **4-5 days**.

Late submissions may not be evaluated.

---

# 🧠 What We Are Looking For

We are assessing your ability to:

- Think like a data engineer
- Handle messy real-world data
- Build scalable systems
- Follow professional Git workflow
- Communicate technical decisions clearly
- Write clean and maintainable code

Good luck.