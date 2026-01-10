# 🚀 BigQuery SQL Analytics — AdventureWorks (Public Dataset)

> **Role focus:** Data Engineer  
> **Tech stack:** Google BigQuery · SQL (Standard SQL) · ELT  
> **Dataset:** AdventureWorks 2019 (Public)

---

## 📌 Table of Contents
- [Project Overview](#-project-overview)
- [Project Goals](#-project-goals)
- [Environment & Dataset](#-environment--dataset)
- [Approach: ETL / ELT Mindset](#-approach-etl--elt-mindset)
  - [Understanding the Source Data](#1-understanding-the-source-data)
  - [Transformation Strategy](#2-transformation-strategy)
  - [Data Quality & Integrity](#3-data-quality--integrity)
- [Key Analytics & Business Use Cases](#-key-analytics--business-use-cases)
- [Data Engineering Skills Demonstrated](#-data-engineering-skills-demonstrated)
- [Repository Structure](#-repository-structure)
- [How to Run](#-how-to-run)
- [Future Improvements](#-future-improvements)

---

## 🔍 Project Overview

This repository demonstrates my ability to design, validate, and optimize **analytical SQL workflows on Google BigQuery** using the **AdventureWorks public dataset**.

Although the data is open-source, I treat this project as a **real-world Data Engineering exercise**:
- understanding raw transactional data,
- defining reliable metrics,
- building scalable SQL transformations,
- and producing analytics-ready outputs that could be consumed by BI tools or downstream data products.

The focus is not only on writing correct SQL, but also on **data integrity, maintainability, and ETL/ELT best practices** in a modern cloud data warehouse.

<img width="759" height="762" alt="image" src="https://github.com/user-attachments/assets/978851bc-2bf3-44d9-a9ad-61b75a5e5e8f" />


---

## 🎯 Project Goals

- Demonstrate strong **SQL proficiency** (CTEs, window functions, ranking, time-series analysis).
- Showcase a **Data Engineering mindset** when transforming raw data into business-ready datasets.
- Handle real-world challenges such as:
  - missing data,
  - inconsistent data types,
  - safe mathematical calculations,
  - and correct join strategies.
- Write queries that are **readable, reproducible, and scalable** in BigQuery.

---

## 🧰 Environment & Dataset

- **Platform:** Google BigQuery  
- **SQL dialect:** BigQuery Standard SQL  
- **Dataset:** `adventureworks2019` (public sample dataset)

### Main data domains
- **Sales:** `SalesOrderDetail`, `SalesOrderHeader`
- **Product:** `Product`, `ProductSubcategory`
- **Inventory / Production:** `WorkOrder`
- **Promotions:** `SpecialOffer`
- **Purchasing:** `PurchaseOrderHeader`

> ⚠️ No proprietary or private data is included in this repository.

---

## 🏗️ Approach: ETL / ELT Mindset

### 1) Understanding the Source Data
Before building transformations, I analyze:
- which tables behave as **facts** (transactions, events),
- which tables behave as **dimensions** (descriptive attributes),
- and which keys are stable and safe to join (`ProductID`, `SalesOrderID`, `CustomerID`).

Example:
- `SalesOrderDetail` → fact table  
- `Product`, `ProductSubcategory` → dimension tables  

This step is critical because an incorrect join type (INNER vs LEFT vs FULL) can silently drop data and distort results.

---

### 2) Transformation Strategy
This project follows a **BigQuery-style ELT approach**:
- Raw data already exists in the warehouse.
- All transformations are done using SQL.

Key design principles:
- **CTE layering (`WITH`)** to break complex logic into readable steps.
- **Consistent aggregation levels** (monthly, yearly, by product, by subcategory).
- **Explicit type casting** using `SAFE_CAST`.
- **Null-safe calculations** using `SAFE_DIVIDE`.

The objective is to create transformations that are easy to audit, debug, and extend.

---

### 3) Data Quality & Integrity
Real datasets are rarely perfect. This project intentionally addresses common issues:

- **Missing values:** handled explicitly, not hidden.
- **Division by zero:** prevented using `SAFE_DIVIDE`.
- **Preserving meaning of NULL:**  
  NULL is not blindly replaced with 0 at the source level if it changes business meaning.
- **Join strategy awareness:**  
  - `LEFT JOIN` for dimensions to avoid losing facts  
  - `FULL OUTER JOIN` where stock or sales may exist independently

This ensures outputs remain trustworthy and explainable.

---

## 📊 Key Analytics & Business Use Cases

The project covers common real-world analytics questions:

### 🔹 Sales Performance
- Last 12 months (L12M) sales by subcategory:
  - quantity sold,
  - total sales value,
  - number of orders.
- Uses the **latest available date** as the time anchor (no hard-coded dates).

### 🔹 Growth Analytics
- Year-over-Year (YoY) growth by subcategory.
- Window functions (`LAG`) to compare current vs previous year.
- **DENSE_RANK** used to correctly rank top performers without skipping ties.

### 🔹 Ranking & Top-N Analysis
- Top 3 territories by order quantity per year.
- Correct handling of ties using window functions.

### 🔹 Promotion Impact
- Seasonal discount cost by subcategory and year.
- Explicit cost calculation for transparency and auditability.

### 🔹 Customer Analytics
- Cohort retention analysis (2014).
- Cohort defined by first purchase month.
- Tracks customer return behavior across subsequent months.

### 🔹 Inventory & Operations
- Monthly stock trends with MoM growth percentage.
- Stock-to-sales ratio analysis:
  - keeps months with stock but no sales (and vice versa),
  - avoids misleading ratios through safe math.

### 🔹 Procurement
- Count and total value of pending purchase orders.

---

## 🧠 Data Engineering Skills Demonstrated

- **Advanced SQL**
  - CTEs, window functions, ranking, time-based calculations
- **ETL / ELT Thinking**
  - transforming raw data into analytics-ready outputs
- **Data Integrity**
  - correct join strategies
  - safe handling of NULLs and edge cases
- **BigQuery Best Practices**
  - Standard SQL
  - scalable query patterns
- **Production Awareness**
  - readable code
  - maintainable structure
  - predictable outputs

---

## 📁 Repository Structure

```text
bigquery-sql-adventureworks/
│
├── README.md
├── sql/
│   └── adventureworks_queries.sql
│
├── docs/
│   └── screenshots/
│
└── .gitignore...
