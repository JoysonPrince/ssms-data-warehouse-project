/*
==================================================================
CREATE DATABASE & SCHEMAS
==================================================================
Script purpose:
This script creates a new Database named 'DataWarehouse' after checking if it already exists.
If the Database exists, it is dropped and recreated. In addition, the Database sets up 3 SCHEMAS: 'bronze', 'silver' and 'gold'.

WARNING:
Running this script will drop the entire database 'DataWarehouse' if it exists.
All data within the database will be permanently deleted.
Proceed with caution and ensure you have proper backups before running this script.

==================================================================
==================================================================
*/

USE master;

-- Drop and Recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
     ALTER DATABASE DataWarehouse SET SINGLE_USER ROLLBACK IMMEDIATE
     DROP DATABASE DataWarehouse;

END;
GO


-- Creating the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;
GO

-- Creating the Schemas: Gold, Silver and Bronze
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
