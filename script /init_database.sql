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

-- ============================================================================
-- SECTION 1: TABLE LOADING - BRONZE STAGING
--            Load CSVs directly into staging (bronze) tables.
-- ============================================================================

-- Set start time for overall process tracking
SET @overall_start = NOW();

-- ----------------------------------------------------------------------------
-- 1.1. LOAD bronze.crm_cust_info_stage
-- ----------------------------------------------------------------------------
SET @start_time = NOW();

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cust_info.csv'
INTO TABLE bronze.crm_cust_info_stage
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@cst_id, @cst_key, @cst_firstname, @cst_lastname, @cst_marital_status, @cst_gndr, @cst_create_date)
SET
  cst_id             = NULLIF(@cst_id, ''),
  cst_key            = NULLIF(@cst_key, ''),
  cst_firstname      = NULLIF(@cst_firstname, ''),
  cst_lastname       = NULLIF(@cst_lastname, ''),
  cst_marital_status = NULLIF(@cst_marital_status, ''),
  cst_gndr           = NULLIF(@cst_gndr, ''),
  cst_create_date    = NULLIF(@cst_create_date, '');

SET @end_time = NOW();
SELECT 
    'crm_cust_info_stage' AS table_name,
    @start_time AS start_time,
    @end_time AS end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS duration_seconds;

-- ----------------------------------------------------------------------------
-- 1.2. LOAD bronze.crm_product_info
-- ----------------------------------------------------------------------------
SET @start_time = NOW();

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/product_info.csv'
INTO TABLE bronze.crm_product_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS
(@prd_id, @prd_key, @prd_name, @prd_cost, @prd_line, @prd_start_date, @prd_end_date)
SET
    prd_id         = NULLIF(@prd_id, ''),
    prd_key        = NULLIF(@prd_key, ''),
    prd_name       = NULLIF(@prd_name, ''),
    prd_cost       = NULLIF(@prd_cost, ''),
    prd_line       = NULLIF(@prd_line, ''),
    prd_start_date = NULLIF(@prd_start_date, ''),
    prd_end_date   = NULLIF(@prd_end_date, '');

SET @end_time = NOW();
SELECT 
    'crm_product_info' AS table_name,
    @start_time AS start_time,
    @end_time AS end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS duration_seconds;

-- ----------------------------------------------------------------------------
-- 1.3. LOAD bronze.crm_sale_info
-- ----------------------------------------------------------------------------
SET @start_time = NOW();

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sales_info.csv'
INTO TABLE bronze.crm_sale_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS
(@sls_ord_num, @sls_prd_key, @sls_cust_id, @sls_order_date, @sls_ship_date, @sls_due_date, @sls_sales, @sls_quantity, @sls_price)
SET
    sls_ord_num     = NULLIF(@sls_ord_num, ''),
    sls_prd_key     = NULLIF(@sls_prd_key, ''),
    sls_cust_id     = NULLIF(@sls_cust_id, ''),
    sls_order_date  = NULLIF(@sls_order_date, ''),
    sls_ship_date   = NULLIF(@sls_ship_date, ''),
    sls_due_date    = NULLIF(@sls_due_date, ''),
    sls_sales       = NULLIF(@sls_sales, ''),
    sls_quantity    = NULLIF(@sls_quantity, ''),
    sls_price       = NULLIF(@sls_price, '');

SET @end_time = NOW();
SELECT 
    'crm_sale_info' AS table_name,
    @start_time AS start_time,
    @end_time AS end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS duration_seconds;

-- ----------------------------------------------------------------------------
-- 1.4. LOAD bronze.crm_CUST_detail
-- ----------------------------------------------------------------------------
SET @start_time = NOW();

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/CUST_detail.csv'
INTO TABLE bronze.crm_CUST_detail
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@Customer_ID, @date, @GENDER)
SET
    Customer_ID = NULLIF(@Customer_ID, ''),
    date        = NULLIF(@date, ''),
    GENDER      = NULLIF(@GENDER, '');

SET @end_time = NOW();
SELECT 
    'crm_CUST_detail' AS table_name,
    @start_time AS start_time,
    @end_time AS end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS duration_seconds;

-- ----------------------------------------------------------------------------
-- 1.5. LOAD bronze.crm_country_id (using LOCAL INFILE, client-side file)
-- ----------------------------------------------------------------------------
SET @start_time = NOW();

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/uploads/country_id.csv'
INTO TABLE bronze.crm_country_id
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@Country_id, @COUNTRY)
SET
    Country_id = NULLIF(@Country_id, ''),
    COUNTRY    = NULLIF(@COUNTRY, '');

SET @end_time = NOW();
SELECT 
    'crm_country_id' AS table_name,
    @start_time AS start_time,
    @end_time AS end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS duration_seconds;

-- ----------------------------------------------------------------------------
-- 1.6. LOAD bronze.crm_product_catalog
-- ----------------------------------------------------------------------------
SET @start_time = NOW();

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/product_catalog.csv'
INTO TABLE bronze.crm_product_catalog
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS
(@Product_ID, @Categor, @SubCategory, @MAINTENANCE)
SET
    Product_ID    = NULLIF(@Product_ID, ''),
    Categor       = NULLIF(@Categor, ''),
    SubCategory   = NULLIF(@SubCategory, ''),
    MAINTENANCE   = NULLIF(@MAINTENANCE, '');

SET @end_time = NOW();
SELECT 
    'crm_product_catalog' AS table_name,
    @start_time AS start_time,
    @end_time AS end_time,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS duration_seconds;

-- ----------------------------------------------------------------------------
-- Show total duration for all loads
-- ----------------------------------------------------------------------------
SET @overall_end = NOW();
SELECT 
    'OVERALL' AS table_name,
    @overall_start AS start_time,
    @overall_end AS end_time,
    TIMESTAMPDIFF(SECOND, @overall_start, @overall_end) AS duration_seconds;


-- ============================================================================
-- SECTION 2: STAGING TO TARGET (Example Procedures)
--            These procedures copy data from staging to target tables.
--            Modify as needed for your actual target tables and transformations.
-- ============================================================================

DELIMITER //

-- ----------------------------------------------------------------------------
-- 2.1. bronze.load_crm_cust_info_stage
--      Copy from staging to target (example: staging table to target table)
--      (In your case, both source and target are the same, so nothing is copied.
--       Adjust to real target table as needed!)
-- ----------------------------------------------------------------------------
CREATE PROCEDURE bronze.load_crm_cust_info_stage()
BEGIN
  -- This example does nothing because source and target are the same!
  -- To move data to a real target table, change the INSERT INTO <target_table>.
  -- Example: If you have bronze.crm_cust_info, use:
  -- INSERT INTO bronze.crm_cust_info (...) SELECT ... FROM bronze.crm_cust_info_stage;

  INSERT INTO bronze.crm_cust_info_stage (
    cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
  )
  SELECT
    cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
  FROM bronze.crm_cust_info_stage;
END //

-- ----------------------------------------------------------------------------
-- 2.2. bronze.load_crm_product_catalog
-- ----------------------------------------------------------------------------
CREATE PROCEDURE bronze.load_crm_product_catalog()
BEGIN
  -- This example demonstrates the pattern for moving data out of staging.
  -- Replace with your actual target table.
  INSERT INTO bronze.crm_product_catalog (
    Product_ID, Categor, SubCategory, MAINTENANCE
  )
  SELECT
    Product_ID, Categor, SubCategory, MAINTENANCE
  FROM bronze.crm_product_catalog_stage;
END //

-- ----------------------------------------------------------------------------
-- 2.3. bronze.load_crm_country_id
-- ----------------------------------------------------------------------------
CREATE PROCEDURE bronze.load_crm_country_id()
BEGIN
  INSERT INTO bronze.crm_country_id (
    Country_id, COUNTRY
  )
  SELECT
    Country_id, COUNTRY
  FROM bronze.crm_country_id_stage;
END //

-- ----------------------------------------------------------------------------
-- 2.4. bronze.load_crm_sale_info
-- ----------------------------------------------------------------------------
CREATE PROCEDURE bronze.load_crm_sale_info()
BEGIN
  INSERT INTO bronze.crm_sale_info (
    sls_ord_num, sls_prd_key, sls_cust_id,
    sls_order_date, sls_ship_date, sls_due_date,
    sls_sales, sls_quantity, sls_price
  )
  SELECT
    sls_ord_num, sls_prd_key, sls_cust_id,
    sls_order_date, sls_ship_date, sls_due_date,
    sls_sales, sls_quantity, sls_price
  FROM bronze.crm_sale_info_stage;
END //

-- ----------------------------------------------------------------------------
-- 2.5. bronze.load_crm_CUST_detail
-- ----------------------------------------------------------------------------
CREATE PROCEDURE bronze.load_crm_CUST_detail()
BEGIN
  INSERT INTO bronze.crm_CUST_detail (
    Customer_ID, date, GENDER
  )
  SELECT
    Customer_ID, date, GENDER
  FROM bronze.crm_CUST_detail_stage;
END //

-- ----------------------------------------------------------------------------
-- 2.6. bronze.load_crm_product_info
-- ----------------------------------------------------------------------------
CREATE PROCEDURE bronze.load_crm_product_info()
BEGIN
  INSERT INTO bronze.crm_product_info (
    prd_id, prd_key, prd_name, prd_cost, prd_line, prd_start_date, prd_end_date
  )
  SELECT
    prd_id, prd_key, prd_name, prd_cost, prd_line, prd_start_date, prd_end_date
  FROM bronze.crm_product_info_stage;
END //

DELIMITER ;

-- ----------------------------------------------------------------------------
-- SECTION 3: EXECUTE STAGING PROCEDURES
--            Call each procedure to copy data from staging to target.
--            Adjust calls according to your actual staging and target setup.
-- ----------------------------------------------------------------------------
CALL bronze.load_crm_cust_info_stage();
CALL bronze.load_crm_product_catalog();
CALL bronze.load_crm_country_id();          -- Note: You had a typo in your original code—only define and call if the procedure exists!
CALL bronze.load_crm_sale_info();
CALL bronze.load_crm_CUST_detail();
CALL bronze.load_crm_product_info();

-- ============================================================================
-- SECTION 4: REPOSITORY NOTES
-- ============================================================================

-- **Author:**       [Your Name]
-- **Repository:**   https://github.com/yourusername/your-repo-name
-- **Description:**  This script loads multiple CSV files into MySQL Bronze layer tables,
--                   then copies data to target tables using stored procedures.
--                   It also records timing for performance tracking.
--
-- **Usage:**        Run all sections in order. Adjust CSV paths as needed for your environment.
--                   Ensure MySQL server and client allow LOAD DATA INFILE/LOCAL.


