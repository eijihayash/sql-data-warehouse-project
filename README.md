# Data Warehouse Project

This repository contains SQL scripts to create and validate a **Data Warehouse** environment following a **multi-layered architecture** (`bronze`, `silver`, and `gold`).  

The project is focused on **data quality checks**, **schema design**, and **fact/dimension table integrity** validation.

---

## Project Structure

- **`create_database.sql`**  
  Script responsible for creating the `DataWarehouse` database and schemas (`bronze`, `silver`, `gold`).  
  If the database already exists, it is dropped and recreated to ensure a clean environment.

- **`tests_quality_checks.sql`**  
  Collection of SQL queries to validate **data quality**:  
  - Detect duplicate records  
  - Validate gender consistency  
  - Check product data integrity  
  - Ensure foreign key relationships in fact tables  

---

## Database Creation

The following actions are performed in `create_database.sql`:

1. **Drop and recreate database**  
   - If `DataWarehouse` exists, it is dropped safely.  
   - A fresh database named `DataWarehouse` is created.  

2. **Create schemas**  
   - `bronze`: raw data layer  
   - `silver`: cleaned and transformed data  
   - `gold`: business-ready data models (facts & dimensions)  

3. **Schema validation**  
   Query to check that schemas were created successfully:  
   ```sql
   SELECT name FROM sys.schemas;

