/*
Create Database and Schema

Script Purpose:
  This script creates a new database named 'DadaWarehouse' after checking if it already exists.
  If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
  within the database: 'bronze', 'silver' ,and 'gold'
*/

USE master;

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;

-- Create the 'DataWarehouse' database

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Create Schemas
CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;

-- Check Schema (dbaver)
SELECT name
FROM sys.schemas;
