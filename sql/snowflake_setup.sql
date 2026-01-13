-- ============================================================
-- Snowflake Setup Script for Retail Data Pipeline
-- ============================================================
-- INSTRUCTIONS:
-- 1. Update YOUR_BUCKET_NAME (line 33)
-- 2. Update YOUR_AWS_ACCESS_KEY (line 36)
-- 3. Update YOUR_AWS_SECRET_KEY (line 37)
-- 4. Copy this entire script into Snowflake Worksheet
-- 5. Click "Run All" or press Ctrl+Enter
-- ============================================================

-- Step 1: Create Database and Schemas
-- ============================================================

CREATE DATABASE IF NOT EXISTS RETAIL_DWH;
USE DATABASE RETAIL_DWH;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- Step 2: Create File Format
-- ============================================================

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE
  COMPRESSION = AUTO;

-- Step 3: Create External Stage (pointing to S3)
-- ============================================================

-- IMPORTANT: Update these three values before running:
CREATE OR REPLACE STAGE s3_stage
  URL = 's3://retail-pipeline-data-YOURNAME/raw/'  -- ← CHANGE THIS to your bucket name
  FILE_FORMAT = csv_format
  CREDENTIALS = (
    AWS_KEY_ID = 'YOUR_AWS_ACCESS_KEY_ID'          -- ← CHANGE THIS to your AWS Access Key
    AWS_SECRET_KEY = 'YOUR_AWS_SECRET_KEY'         -- ← CHANGE THIS to your AWS Secret Key
  );

-- Verify stage connection
LIST @s3_stage;

-- Step 4: Create Raw Tables
-- ============================================================

USE SCHEMA RAW;

-- Stores Table
CREATE OR REPLACE TABLE stores (
    store_id INTEGER,
    store_name VARCHAR(255),
    store_type VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(2),
    country VARCHAR(50),
    opened_date DATE,
    size_sqft INTEGER,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Products Table
CREATE OR REPLACE TABLE products (
    product_id INTEGER,
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(255),
    cost_price DECIMAL(10,2),
    retail_price DECIMAL(10,2),
    supplier VARCHAR(255),
    created_date DATE,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Customers Table
CREATE OR REPLACE TABLE customers (
    customer_id INTEGER,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    signup_date DATE,
    customer_segment VARCHAR(50),
    loyalty_member BOOLEAN,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Transactions Table
CREATE OR REPLACE TABLE transactions (
    transaction_id INTEGER,
    transaction_date DATE,
    transaction_time TIME,
    store_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    payment_method VARCHAR(50),
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Step 5: Load Data from S3 to Raw Tables
-- ============================================================

-- Load Stores
COPY INTO stores (store_id, store_name, store_type, city, state, country, opened_date, size_sqft)
FROM @s3_stage/stores/
FILE_FORMAT = csv_format
PATTERN = '.*stores.*.csv'
ON_ERROR = 'CONTINUE';

-- Load Products
COPY INTO products (product_id, product_name, category, subcategory, brand, cost_price, retail_price, supplier, created_date)
FROM @s3_stage/products/
FILE_FORMAT = csv_format
PATTERN = '.*products.*.csv'
ON_ERROR = 'CONTINUE';

-- Load Customers
COPY INTO customers (customer_id, first_name, last_name, email, phone, address, city, state, zip_code, signup_date, customer_segment, loyalty_member)
FROM @s3_stage/customers/
FILE_FORMAT = csv_format
PATTERN = '.*customers.*.csv'
ON_ERROR = 'CONTINUE';

-- Load Transactions
COPY INTO transactions (transaction_id, transaction_date, transaction_time, store_id, customer_id, product_id, quantity, unit_price, discount_amount, total_amount, payment_method)
FROM @s3_stage/transactions/
FILE_FORMAT = csv_format
PATTERN = '.*transactions.*.csv'
ON_ERROR = 'CONTINUE';

-- Step 6: Verify Data Load
-- ============================================================

SELECT 'Stores' AS table_name, COUNT(*) AS row_count FROM stores
UNION ALL
SELECT 'Products', COUNT(*) FROM products
UNION ALL
SELECT 'Customers', COUNT(*) FROM customers
UNION ALL
SELECT 'Transactions', COUNT(*) FROM transactions;

-- Sample queries to verify data
SELECT * FROM stores LIMIT 10;
SELECT * FROM products LIMIT 10;
SELECT * FROM customers LIMIT 10;
SELECT * FROM transactions LIMIT 10;

-- Step 7: Create Warehouse (if not exists)
-- ============================================================

CREATE WAREHOUSE IF NOT EXISTS RETAIL_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = FALSE;

USE WAREHOUSE RETAIL_WH;

-- Step 8: Grant Permissions (Optional - for multi-user setup)
-- ============================================================

-- Create role for dbt (optional)
CREATE ROLE IF NOT EXISTS DBT_ROLE;
GRANT USAGE ON DATABASE RETAIL_DWH TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA RETAIL_DWH.RAW TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA RETAIL_DWH.STAGING TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA RETAIL_DWH.ANALYTICS TO ROLE DBT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA RETAIL_DWH.RAW TO ROLE DBT_ROLE;
GRANT ALL ON SCHEMA RETAIL_DWH.STAGING TO ROLE DBT_ROLE;
GRANT ALL ON SCHEMA RETAIL_DWH.ANALYTICS TO ROLE DBT_ROLE;
GRANT USAGE ON WAREHOUSE RETAIL_WH TO ROLE DBT_ROLE;

-- ============================================================
-- Setup Complete! 🎉
-- 
-- Expected Results:
-- - Stores: ~50 rows
-- - Products: ~500 rows
-- - Customers: ~10,000 rows
-- - Transactions: ~150,000+ rows
--
-- Next Steps:
-- 1. Set up dbt project
-- 2. Run dbt transformations
-- 3. Launch Streamlit dashboard
-- ============================================================