# SSMS-Data-Warehouse-project

Welcome to the **Data WareHouse and Analytics project** repository.

## Main goal: 
Building a modern data warehouse using MS SQL Server including ETL, Data modeling and Analytics - using a Medallion Architecture

1. This project demonstrates a comprehensive data warehousing and analytics solution. From building a Data Warehouse to generating actionable insights.
2. This project is designed as a portfolio project to showcase my skills as it highlights industry best practices in data engineering and analytics.


## Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---
## ETL concepts & techniques used:
  ### EXTRACTION:
  1. EXTRACTION Technique: ***FILE PARSING:*** *Data parsing from csv files located within the local machine folder*
  2. EXTRACTION Type: ***FULL EXTRACTION:*** *FULL Data load everyday into the Data warehouse*
  3. EXTRACTION Method: ***PULL EXTRACTION:*** *Pull data from source*

  ### TRANSFORMATION:
  #### Transformation techniques:
  1. Data Enrichment
  2. Data Integration
  3. Derived columns
  4. Data Normalization and Data standardization
  5. Data aggregations
  6. Applying business rules and logic

  #### Data cleansing:
  1. Removed duplicates: Data quality check on the PRIMARY KEY
  2. Data filtering
  3. Handling missing values: NULL, ZERO VALUES, etc.
  4. Handling invalid values
  5. Handling unwanted whitespaces
  6. Data Type Casting
  7. Outlier detection
  
  ### LOAD:
  1. LOAD Processing type: ***BATCH PROCESSING:*** *Loading data in a single batch*
  2. LOAD Method: ***FULL LOAD: TRUNCATE & INSERT***


### BI: Analytics & Reporting (Data Analysis)

#### Objective
Develop SQL-based analytics to deliver detailed insights into:
- **Customer Behavior**
- **Product Performance**
- **Sales Trends**

These insights empower stakeholders with key business metrics, enabling strategic decision-making.

## License:
This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.


