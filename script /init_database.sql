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

