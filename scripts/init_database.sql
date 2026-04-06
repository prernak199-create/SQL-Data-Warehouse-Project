/*
Create database and schemas
Script Purpose:
This script creates a new datbase named 'Datewarehouse' .
Additionaly the script has three schemas  within the database: 'bronze','silver' and 'gold'
*/



USE master;

CREATE DATABASE DataWarehouse;

Use DataWarehouse;
GO

 CREATE SCHEMA bronze;
 GO
 CREATE SCHEMA silver;
 GO
 CREATE SCHEMA gold;
 GO
