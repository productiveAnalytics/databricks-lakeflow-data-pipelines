-- Create Unity Catalog for Lakeflow SCD Demo
-- This script creates the catalog and grants necessary permissions
-- Run this BEFORE deploying the bundle

-- Create catalog if it doesn't exist
CREATE CATALOG IF NOT EXISTS lakeflow_scd_demo
COMMENT 'Catalog for Lakeflow SCD Type 1 and Type 2 reference implementations';

-- Use the catalog
USE CATALOG lakeflow_scd_demo;

-- Grant permissions to current user (replace with your user/group as needed)
-- These grants allow the current user to:
--   - Use the catalog
--   - Create schemas
--   - Create tables and volumes
--   - Run pipelines that write to this catalog

GRANT USE CATALOG ON CATALOG lakeflow_scd_demo TO `lalitstar@gmail.com`;
GRANT CREATE SCHEMA ON CATALOG lakeflow_scd_demo TO `lalitstar@gmail.com`;
GRANT USE SCHEMA ON CATALOG lakeflow_scd_demo TO `lalitstar@gmail.com`;

-- Optional: Grant to a group (uncomment and modify as needed)
-- GRANT USE CATALOG ON CATALOG lakeflow_scd_demo TO `data_engineers`;
-- GRANT CREATE SCHEMA ON CATALOG lakeflow_scd_demo TO `data_engineers`;
-- GRANT USE SCHEMA ON CATALOG lakeflow_scd_demo TO `data_engineers`;

-- Verify catalog creation
DESCRIBE CATALOG lakeflow_scd_demo;
