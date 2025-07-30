/*
==========================================
🎯 Project: Modern SQL Data Warehouse
🗂️  Architecture: Medallion (Bronze, Silver, Gold)
🛠️  Tech: MySQL / SQL Server Compatible
📅  Author: Abhay Rana
📄  Description:
    This script sets up the initial structure for a 
    Data Warehouse using the Medallion architecture.
    It creates a dedicated database and separate schemas 
    for each processing stage: Bronze, Silver, and Gold.
==========================================
*/

IF EXISTS (SELECT * FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
    ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE Datawarehouse;
END
-- Create a master database for admin-level use (optional)
CREATE DATABASE master;

-- Switch to the master database
USE master;


-- Create the main Data Warehouse database
CREATE DATABASE Datawarehouse;

-- Use the Datawarehouse for all schema creations
USE Datawarehouse;

-- Create the Bronze schema: raw data layer
CREATE SCHEMA bronze;

-- Create the Silver schema: cleaned and transformed data
CREATE SCHEMA silver;

-- Create the Gold schema: business-level aggregated data
-- Drop and recreate customer info staging table
DROP TABLE IF EXISTS bronze.crm_cust_info_stage;
CREATE TABLE bronze.crm_cust_info_stage (
    cst_id INT DEFAULT NULL,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(10),
    cst_create_date VARCHAR(20)  -- Stored as string temporarily
);

-- Drop and recreate product info table
DROP TABLE IF EXISTS bronze.crm_product_info;
CREATE TABLE bronze.crm_product_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_name VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(20),
    prd_start_date INT,
    prd_end_date INT
);

-- Drop and recreate sales info table
DROP TABLE IF EXISTS bronze.crm_sale_info;
CREATE TABLE bronze.crm_sale_info (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_date INT,
    sls_ship_date INT,
    sls_due_date INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

-- Drop and recreate customer info table
DROP TABLE IF EXISTS bronze.crm_customer_info;
CREATE TABLE bronze.crm_customer_info (
    Customer_ID INT,
    date INT,
    GENDER VARCHAR(10)
);

-- Drop and recreate country table
DROP TABLE IF EXISTS bronze.crm_country_id;
CREATE TABLE bronze.crm_country_id (
    Country_id VARCHAR(50),
    COUNTRY VARCHAR(50)
);

-- Drop and recreate product catalog table
DROP TABLE IF EXISTS bronze.crm_product_catalog;
CREATE TABLE bronze.crm_product_catalog (
    Product_ID INT,
    Categor VARCHAR(50),
    SubCategory VARCHAR(50),
    MAINTENANCE VARCHAR(50)
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cust_info.csv'
INTO TABLE bronze.crm_cust_info_stage
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@cst_id, @cst_key, @cst_firstname, @cst_lastname, @cst_marital_status, @cst_gndr, @cst_create_date)
SET
  cst_id = NULLIF(@cst_id, ''),
  cst_key = NULLIF(@cst_key, ''),
  cst_firstname = NULLIF(@cst_firstname, ''),
  cst_lastname = NULLIF(@cst_lastname, ''),
  cst_marital_status = NULLIF(@cst_marital_status, ''),
  cst_gndr = NULLIF(@cst_gndr, ''),
  cst_create_date = NULLIF(@cst_create_date, '');

-- Check total rows inserted
SELECT COUNT(*) FROM bronze.crm_cust_info_stage;


