# 🛠️ Retail Order ETL Project

Welcome to the **Retail Order ETL Project** repository! 🚀
This project demonstrates a complete ETL (Extract, Transform, Load) pipeline to process and load retail order data from multiple CSV files into a PostgreSQL relational database using Python.

---
## 📖 Project Overview
This project involves:
1. **Data Architecture**: Lays the foundation for scalable data warehousing using principles aligned with Medallion Architecture — organizing data into Bronze (raw), Silver (cleaned), and Gold (aggregated/reporting) layers.
2. **ETL Pinelines**: Using Python scripts to extract data from source CSVs, transform and clean it, and load it into PostgreSQL.
3. **Data Modeling**: Implements a relational schema with fact and dimension tables optimized for analytics, ensuring data integrity with foreign keys and normalization.
4. **Analytics & Reporting**: Enables SQL-based insights on sales, customers, and operations with queries and future dashboard plans.

---

## 🚀 Project Requirements

### Building a retail ETL data pipeline (Data Engineering)

---
## 📦 Technology Used
- **Python** (pandas, psycopg2, dotenv)
- **PostgreSQL** (local database)
- **pgAdmin** for GUI access
- **draw.io** for data modeling
- **VS Code** for development

---
## 🗂️ Folder Structure
```text
retail-etl-pipeline/
├── datasets/
│   ├── rawData/                 # Input data (CSV/JSON)
│   └── processedData/           # Cleaned/transformed data
├── diagrams/
│   └── data_model.drawio        # ER diagram for database tables
├── etl/
│   ├── extract.py               # Read data from raw files
│   ├── transform.py             # Clean, join, enrich data
│   └── load.py                  # Load data into PostgreSQL
└── scripts/
    └── main.py                  # Orchestrates the full ETL
├── README.md
├── .env                         # DB credentials (never commit to GitHub)
├── requirements.txt
```
---
##  ⚙️ ETL Workflow (Retail Scenario)
### 1. Extract 
- Load 5 raw CSV files: customers, orders, orderitems, products, payments
- Preview and analyze the structure

### 2. Transform 
- Normalize column formats (upper/lower case, strip spaces)
- Convert timestamps to datetime
- Drop nulls and duplicates
- Save cleaned versions to data/clean_data/

### 3. Load
- Create PostgreSQL tables with foreign key constraints
- Load each table in the correct dependency order
- Skip invalid FK rows and log skipped rows

### 4. Data Validation
- Check row counts per table
- Detect null values in each column
- Compare database vs CSV rows
  
---
### 🚀 How to Run
1. Set up and activate Python virtual environment:
  - MacOS/Linux: python3 -m venv env, then source env/bin/activate
  - Windows: python virtualenv env, then ./env/scripts/activate
2. Install required packages: pip install -r requirements.txt
3. Set up PostgreSQL
  - Create a database: retail_order_db
  - Create tables using pgAdmin or schema.sql
  - Add .env file and set the environment variables
4. Run pipeline

---
##  Author
**Ngan Huynh**

Data Engineering Student & ETL Builder

---
## 📝 License
MIT License

