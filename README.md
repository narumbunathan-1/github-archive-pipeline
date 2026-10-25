# 🌐 Global GitOps & Threat Intelligence Data Platform

An end-to-end, automated Data Engineering pipeline that ingests massive volumes of real-time GitHub activity data, models it using an **Enterprise Data Vault 2.0** architecture, and serves actionable Threat Intelligence and Developer Metrics.

## 🚀 The Business Problem
GitHub generates millions of events daily. Querying this data in its raw, nested JSON format is slow, expensive, and difficult to track over time. 

Originally, this project utilized a flat "Hub and Spoke" data model with 15 standard views (e.g., `VW_COMMITS`, `VW_ISSUES`). While functional, it lacked historical tracking and resulted in massive data duplication. 

**The Solution:** This project upgraded that architecture into a highly modular **Data Vault 2.0** platform using **dbt**, ensuring 100% historical auditability, zero data loss, and enterprise-grade scalability.

---

## 🏗️ System Architecture & Data Flow

### 1. Data Extraction (Apache Airflow)
* **Where it lives:** `airflow_dags/hourly_gharchive_elt.py`
* **What it does:** Every hour, Airflow connects to the public GitHub Archive API. It downloads massive `.json.gz` files, splits them into smaller, memory-safe chunks on the local Linux machine, and securely pushes them to a Snowflake Internal Stage using RSA key-pair authentication.

### 2. The Landing Zone (Snowflake Bronze)
* **What it does:** Snowflake uses a `COPY INTO` command to ingest the JSON chunks from the internal stage directly into a Bronze table (`RAW_EVENTS`). The complex JSON is kept entirely intact inside a Snowflake `VARIANT` column so no raw data is ever lost.

### 3. Enterprise Transformation (dbt Silver - Data Vault 2.0)
* **Where it lives:** `dbt_project/models/vault/`
* **What it does:** This is the core engine of the project. It uses dbt to shred the nested JSON into three specific types of tables:
  * **Hubs (The Entities):** Extracts unique business keys into `hub_user`, `hub_repo`, and `hub_event`. We use `MD5()` hashing and `QUALIFY ROW_NUMBER()` to guarantee absolute uniqueness.
  * **Links (The Transactions):** Creates a mathematical bridge (`link_user_repo_event`) proving *which* user performed *what* event on *which* repository.
  * **Satellites (The Details):** Splits the raw JSON into 16 distinct tables based on the event type (e.g., `sat_event_commits`, `sat_event_issues`). This replaces the old flat views, preventing millions of `NULL` columns and tracking historical changes perfectly.

### 4. Business Intelligence (dbt Gold)
* **Where it lives:** `dbt_project/models/gold/`
* **What it does:** These models join the Data Vault (Hubs + Links + Satellites) back together to serve clean, aggregated metrics for visualization tools like Streamlit or Metabase:
  * `gold_developer_metrics`: Tracks PRs, code volume, and review activity.
  * `gold_repo_trends`: Calculates a "Health Score" based on stars, forks, and bugs.
  * `gold_threat_alerts`: Detects anomalies, like massive commit volumes from new users right before a repository status change.

---

## 🛡️ Data Quality & CI/CD
* **Automated Testing:** The `schema.yml` file uses dbt to automatically test for duplicates and enforce Foreign Key referential integrity (ensuring no orphaned records exist between Links and Hubs).
* **GitHub Actions:** Every push to the `main` branch triggers a CI/CD pipeline (`.github/workflows/dbt_ci.yml`) that compiles the dbt project and runs the test suite against Snowflake to prevent bad code from reaching production.

---

## 📂 Repository Structure Guide

```text
github-archive-pipeline/
├── .github/workflows/
│   └── dbt_ci.yml                # Automated CI/CD pipeline for GitHub Actions
├── airflow_dags/
│   └── hourly_gharchive_elt.py   # Airflow Python script for Extraction/Loading
└── dbt_project/
    ├── models/
    │   ├── staging/              # Unpacks the raw Snowflake JSON
    │   ├── vault/
    │   │   ├── hubs/             # Core Entities (hub_user, hub_repo, hub_event)
    │   │   ├── links/            # Transactional Bridges (link_user_repo_event)
    │   │   └── satellites/       # 16 specific payload tables (sat_event_commits, etc.)
    │   └── gold/                 # Aggregated Analytics & Threat Alerts for BI
    ├── dbt_project.yml           # Core dbt configuration file
    └── schema.yml                # Data Quality Assertions and Tests
