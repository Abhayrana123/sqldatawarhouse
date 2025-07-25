This project showcases the development of a modern SQL-based Data Warehouse using the Medallion Architecture (Bronze, Silver, Gold) to organize and transform data efficiently for analytics and reporting purposes.

🧱 Medallion Architecture
🔷 Bronze Layer – Raw Data Ingestion
Collected raw data from CSV files and simulated APIs

Loaded directly into staging tables with minimal changes

Ensured full data capture for audit and reprocessing

🟡 Silver Layer – Clean & Transformed Data
Applied data cleaning: removed duplicates, standardized formats

Implemented joins and business logic to create clean data views

Normalized structure for flexibility and data consistency

💎 Gold Layer – Analytics-Ready Data
Built fact and dimension tables for reporting

Aggregated data for KPIs and dashboards

Optimized queries for speed using indexes and views

⚙️ Tools & Technologies
SQL (PostgreSQL / MySQL / BigQuery – depending on setup)

dbt (for data modeling and transformations) (optional)

Python (for ETL orchestration and file handling) (optional)

Power BI / Tableau (for data visualization) 

👨‍💻 How I Worked on This Project
Step 1: Requirement Understanding
Analyzed the needs of a business analytics system and planned the warehouse schema accordingly.

Step 2: Data Ingestion
Wrote SQL scripts to load raw datasets into the Bronze layer, ensuring data preservation and logging.

Step 3: Data Transformation
Cleaned and transformed data in the Silver layer using SQL procedures and CTEs.

Step 4: Data Modeling
Designed fact and dimension tables using the star schema in the Gold layer to support business queries.

Step 5: Testing & Optimization
Validated the data quality, tested joins, and optimized query performance with indexes and views.

Step 6: Documentation
Added schema diagrams, clear comments in SQL files, and prepared a README for better project understanding.
MIT License

Copyright (c) 2025 Abhay

Permission is hereby granted, free of charge, to any person obtaining a copy  
of this software and associated documentation files (the "Software"), to deal  
in the Software without restriction, including without limitation the rights  
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell  
copies of the Software, and to permit persons to whom the Software is  
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in  
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR  
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,  
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE  
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER  
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,  
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE  
SOFTWARE.




