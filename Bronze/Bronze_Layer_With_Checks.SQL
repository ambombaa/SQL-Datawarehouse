/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);
GO

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
GO

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO




/************************************************************************************************************************

    Load Scripts - To load data from CSV's into created tables

-------------------------------------------------------------------------------------------------------------------------
       
       SYNTAX:
                BULK INSERT <Schema>.<TableName>
                FROM        <Path to the File>

                WITH
                (
                    FIRSTROW  = 2,          ----- Row to start reading data from
                    
                    FIELDTERMINATOR = ',',  ----- The separator
                    
                    TABLOCK                 ----- Telling SQL to only focus into 1 table
                );

---------------------------------------------------------------------------------------------------------------------------


        NB: 
            - If you run the query above twice, data will be duplicated.
            - To avoid this, add the following syntax before BULK INSERT
                
                TRUNCATE TABLE <Schema>.<Table>


       ---------------------------------------------- Final Syntax ------------------------------------------------------

        TRUNCATE TABLE bronze.crm_cust_info

        BULK INSERT bronze.crm_cust_info
        
        FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'

        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

*************************************************************************************************************************/

Use Datawarehouse   ---- Selecting the database


TRUNCATE TABLE bronze.crm_cust_info

BULK INSERT bronze.crm_cust_info

FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'

WITH
(
    FIRSTROW = 2,           ------------- Telling SQL to data reading data from 2nd Row
    FIELDTERMINATOR = ',',  ------------- SQL File is separated by commas
    TABLOCK                 ------------- Telling SQL to lock the table to this table when importing the data
);



/***************************************************************************************************************************
    
    Quality Check after loading:
        - Check if all data was imported matching to their column.
            
            - SYNTAX:
                        SELECT  *
                        
                        FROM    <Table>

        - Check if all Rows where imported
                
            - SYNTAX
                        SELECT  Count(*)
                        
                        FROM    <Table>

****************************************************************************************************************************/

SELECT *

FROM    bronze.crm_cust_info



SELECT  Count(*)

FROM    bronze.crm_cust_info



---------------------------------------------- BULK INSERT for Table bronze.crm_prd_info -----------------------------------------


TRUNCATE TABLE [bronze].[crm_prd_info];

BULK INSERT [bronze].[crm_prd_info]
FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH
(
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
);


---------------------------------------------- BULK INSERT for Table bronze.crm._sales_details -----------------------------------------


TRUNCATE TABLE [bronze].[crm_sales_details];

BULK INSERT [bronze].[crm_sales_details]
FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH
(
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
);


---------------------------------------------- BULK INSERT for Table bronze.erp_cust_az12 -----------------------------------------


TRUNCATE TABLE [bronze].[erp_cust_az12];

BULK INSERT [bronze].[erp_cust_az12]
FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH
(
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
);



---------------------------------------------- BULK INSERT for Table bronze.erp_loc_a101 -----------------------------------------


TRUNCATE TABLE [bronze].[erp_loc_a101];

BULK INSERT [bronze].[erp_loc_a101]
FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH
(
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
);



---------------------------------------------- BULK INSERT for Table bronze.erp_px_cat_g1v2 -----------------------------------------


TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];

BULK INSERT [bronze].[erp_px_cat_g1v2]
FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH
(
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
);





/**************************************************************************************************************************************

    Build Stored Procedure:

            - Here we simply going to stored procedure of all LOAD SCRIPTS we wrote.

***************************************************************************************************************************************/



CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN

---------------------------------------------- BULK INSERT for Table bronze.crm_cust_info -----------------------------------------

TRUNCATE TABLE bronze.crm_cust_info

BULK INSERT bronze.crm_cust_info

FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'

WITH
(
    FIRSTROW = 2,           ------------- Telling SQL to data reading data from 2nd Row
    FIELDTERMINATOR = ',',  ------------- SQL File is separated by commas
    TABLOCK                 ------------- Telling SQL to lock the table to this table when importing the data
);


---------------------------------------------- BULK INSERT for Table bronze.crm_prd_info -----------------------------------------


TRUNCATE TABLE [bronze].[crm_prd_info];

BULK INSERT [bronze].[crm_prd_info]
FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH
(
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
);


---------------------------------------------- BULK INSERT for Table bronze.crm._sales_details -----------------------------------------


TRUNCATE TABLE [bronze].[crm_sales_details];

BULK INSERT [bronze].[crm_sales_details]
FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH
(
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
);


---------------------------------------------- BULK INSERT for Table bronze.erp_cust_az12 -----------------------------------------


TRUNCATE TABLE [bronze].[erp_cust_az12];

BULK INSERT [bronze].[erp_cust_az12]
FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH
(
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
);



---------------------------------------------- BULK INSERT for Table bronze.erp_loc_a101 -----------------------------------------


TRUNCATE TABLE [bronze].[erp_loc_a101];

BULK INSERT [bronze].[erp_loc_a101]
FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH
(
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
);



---------------------------------------------- BULK INSERT for Table bronze.erp_px_cat_g1v2 -----------------------------------------


TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];

BULK INSERT [bronze].[erp_px_cat_g1v2]
FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH
(
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
);

END




-------------------- To Check or Execugte the STORED PROCEDURE bronze.load_bronze -----------------------


EXEC bronze.load_bronze




/**************************************************************************************************************************************

    Build Stored Procedure (CONTINUATION):

            - Add Messages on the code in order to have much clear and understandable output.
            - We do that by adding manual display Messages and Separators

                Example: 
                    PRINT '==========================='            Separator
                    PRINT 'Loading of the bronze Layer'            Message

***************************************************************************************************************************************/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN


PRINT '================================================================================================================================';
PRINT 'Loading the Bronze Layer'
PRINT '================================================================================================================================';




PRINT '---------------------------------------------------------------------------------------------------------------------------------';
PRINT 'Loading of CRM tables'
PRINT '---------------------------------------------------------------------------------------------------------------------------------';



---------------------------------------------- BULK INSERT for Table bronze.crm_cust_info -----------------------------------------




PRINT '>>>>>>>>>>>>>>> Truncating Table: bronze.crm_cust_info <<<<<<<<<<<<<<<<<< ';

TRUNCATE TABLE bronze.crm_cust_info


PRINT '>>>>>>>>>>>>>>> Inserting data into: bronze.crm_cust_info <<<<<<<<<<<<<<<<<< ';
BULK INSERT bronze.crm_cust_info

FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'

WITH
(
    FIRSTROW = 2,           ------------- Telling SQL to data reading data from 2nd Row
    FIELDTERMINATOR = ',',  ------------- SQL File is separated by commas
    TABLOCK                 ------------- Telling SQL to lock the table to this table when importing the data
);





---------------------------------------------- BULK INSERT for Table bronze.crm_prd_info -----------------------------------------


PRINT '>>>>>>>>>>>>>>> Truncating Table: [bronze].[crm_prd_info] <<<<<<<<<<<<<<<<<< ';

TRUNCATE TABLE [bronze].[crm_prd_info];

PRINT '>>>>>>>>>>>>>>> Inserting data into: [bronze].[crm_prd_info] <<<<<<<<<<<<<<<<<< ';

BULK INSERT [bronze].[crm_prd_info]
FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH
(
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
);

---------------------------------------------- BULK INSERT for Table bronze.crm._sales_details -----------------------------------------

PRINT '>>>>>>>>>>>>>>> Truncating Table: [bronze].[crm_sales_details] <<<<<<<<<<<<<<<<<< ';

TRUNCATE TABLE [bronze].[crm_sales_details];

PRINT '>>>>>>>>>>>>>>> Inserting data into: [bronze].[crm_sales_details] <<<<<<<<<<<<<<<<<< ';

BULK INSERT [bronze].[crm_sales_details]
FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH
(
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
);



PRINT '---------------------------------------------------------------------------------------------------------------------------------';
PRINT 'Loading of ERP tables'
PRINT '---------------------------------------------------------------------------------------------------------------------------------';


---------------------------------------------- BULK INSERT for Table bronze.erp_cust_az12 -----------------------------------------


PRINT '>>>>>>>>>>>>>>> Truncating Table: [bronze].[erp_cust_az12] <<<<<<<<<<<<<<<<<< ';

TRUNCATE TABLE [bronze].[erp_cust_az12];

PRINT '>>>>>>>>>>>>>>> Inserting data into: [bronze].[erp_cust_az12] <<<<<<<<<<<<<<<<<< ';

BULK INSERT [bronze].[erp_cust_az12]
FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH
(
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
);



---------------------------------------------- BULK INSERT for Table bronze.erp_loc_a101 -----------------------------------------


PRINT '>>>>>>>>>>>>>>> Truncating Table: [bronze].[erp_loc_a101] <<<<<<<<<<<<<<<<<< ';
TRUNCATE TABLE [bronze].[erp_loc_a101];

PRINT '>>>>>>>>>>>>>>> Inserting data into: [bronze].[erp_loc_a101] <<<<<<<<<<<<<<<<<< ';
BULK INSERT [bronze].[erp_loc_a101]
FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH
(
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
);


---------------------------------------------- BULK INSERT for Table bronze.erp_px_cat_g1v2 -----------------------------------------

PRINT '>>>>>>>>>>>>>>> Truncating Table: [bronze].[erp_px_cat_g1v2] <<<<<<<<<<<<<<<<<< ';
TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];


PRINT '>>>>>>>>>>>>>>> Inserting data into: [bronze].[erp_px_cat_g1v2] <<<<<<<<<<<<<<<<<< ';
BULK INSERT [bronze].[erp_px_cat_g1v2]
FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH
(
       FIRSTROW = 2,
       FIELDTERMINATOR = ',',
       TABLOCK
);

END



/*******************************************************************************************************************************************

    STORED PROCEDURE
         - TRY ..... CATCH
            - Used to handle errors in Stored procedures 

*******************************************************************************************************************************************/



CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN

    BEGIN TRY
    ---------------------------------------------- BULK INSERT for Table bronze.crm_cust_info -----------------------------------------

    TRUNCATE TABLE bronze.crm_cust_info

    BULK INSERT bronze.crm_cust_info

    FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'

    WITH
    (
        FIRSTROW = 2,           ------------- Telling SQL to data reading data from 2nd Row
        FIELDTERMINATOR = ',',  ------------- SQL File is separated by commas
        TABLOCK                 ------------- Telling SQL to lock the table to this table when importing the data
    );


    ---------------------------------------------- BULK INSERT for Table bronze.crm_prd_info -----------------------------------------


    TRUNCATE TABLE [bronze].[crm_prd_info];

    BULK INSERT [bronze].[crm_prd_info]
    FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH
    (
           FIRSTROW = 2,
           FIELDTERMINATOR = ',',
           TABLOCK
    );


    ---------------------------------------------- BULK INSERT for Table bronze.crm._sales_details -----------------------------------------


    TRUNCATE TABLE [bronze].[crm_sales_details];

    BULK INSERT [bronze].[crm_sales_details]
    FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH
    (
           FIRSTROW = 2,
           FIELDTERMINATOR = ',',
           TABLOCK
    );


    ---------------------------------------------- BULK INSERT for Table bronze.erp_cust_az12 -----------------------------------------


    TRUNCATE TABLE [bronze].[erp_cust_az12];

    BULK INSERT [bronze].[erp_cust_az12]
    FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH
    (
           FIRSTROW = 2,
           FIELDTERMINATOR = ',',
           TABLOCK
    );



    ---------------------------------------------- BULK INSERT for Table bronze.erp_loc_a101 -----------------------------------------


    TRUNCATE TABLE [bronze].[erp_loc_a101];

    BULK INSERT [bronze].[erp_loc_a101]
    FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH
    (
           FIRSTROW = 2,
           FIELDTERMINATOR = ',',
           TABLOCK
    );



    ---------------------------------------------- BULK INSERT for Table bronze.erp_px_cat_g1v2 -----------------------------------------


    TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];

    BULK INSERT [bronze].[erp_px_cat_g1v2]
    FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH
    (
           FIRSTROW = 2,
           FIELDTERMINATOR = ',',
           TABLOCK
    );

    END




    -------------------- To Check or Execugte the STORED PROCEDURE bronze.load_bronze -----------------------


    EXEC bronze.load_bronze




    /**************************************************************************************************************************************

        Build Stored Procedure (CONTINUATION):

                - Add Messages on the code in order to have much clear and understandable output.
                - We do that by adding manual display Messages and Separators

                    Example: 
                        PRINT '==========================='            Separator
                        PRINT 'Loading of the bronze Layer'            Message

    ***************************************************************************************************************************************/


    CREATE OR ALTER PROCEDURE bronze.load_bronze AS

    BEGIN

    BEGIN TRY

            PRINT '================================================================================================================================';
            PRINT 'Loading the Bronze Layer'
            PRINT '================================================================================================================================';




            PRINT '---------------------------------------------------------------------------------------------------------------------------------';
            PRINT 'Loading of CRM tables'
            PRINT '---------------------------------------------------------------------------------------------------------------------------------';



            ---------------------------------------------- BULK INSERT for Table bronze.crm_cust_info -----------------------------------------




            PRINT '>>>>>>>>>>>>>>> Truncating Table: bronze.crm_cust_info <<<<<<<<<<<<<<<<<< ';

            TRUNCATE TABLE bronze.crm_cust_info


            PRINT '>>>>>>>>>>>>>>> Inserting data into: bronze.crm_cust_info <<<<<<<<<<<<<<<<<< ';
            BULK INSERT bronze.crm_cust_info

            FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'

            WITH
            (
                FIRSTROW = 2,           ------------- Telling SQL to data reading data from 2nd Row
                FIELDTERMINATOR = ',',  ------------- SQL File is separated by commas
                TABLOCK                 ------------- Telling SQL to lock the table to this table when importing the data
            );





            ---------------------------------------------- BULK INSERT for Table bronze.crm_prd_info -----------------------------------------


            PRINT '>>>>>>>>>>>>>>> Truncating Table: [bronze].[crm_prd_info] <<<<<<<<<<<<<<<<<< ';

            TRUNCATE TABLE [bronze].[crm_prd_info];

            PRINT '>>>>>>>>>>>>>>> Inserting data into: [bronze].[crm_prd_info] <<<<<<<<<<<<<<<<<< ';

            BULK INSERT [bronze].[crm_prd_info]
            FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
            WITH
            (
                   FIRSTROW = 2,
                   FIELDTERMINATOR = ',',
                   TABLOCK
            );

            ---------------------------------------------- BULK INSERT for Table bronze.crm._sales_details -----------------------------------------

            PRINT '>>>>>>>>>>>>>>> Truncating Table: [bronze].[crm_sales_details] <<<<<<<<<<<<<<<<<< ';

            TRUNCATE TABLE [bronze].[crm_sales_details];

            PRINT '>>>>>>>>>>>>>>> Inserting data into: [bronze].[crm_sales_details] <<<<<<<<<<<<<<<<<< ';

            BULK INSERT [bronze].[crm_sales_details]
            FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
            WITH
            (
                   FIRSTROW = 2,
                   FIELDTERMINATOR = ',',
                   TABLOCK
            );



            PRINT '---------------------------------------------------------------------------------------------------------------------------------';
            PRINT 'Loading of ERP tables'
            PRINT '---------------------------------------------------------------------------------------------------------------------------------';


            ---------------------------------------------- BULK INSERT for Table bronze.erp_cust_az12 -----------------------------------------


            PRINT '>>>>>>>>>>>>>>> Truncating Table: [bronze].[erp_cust_az12] <<<<<<<<<<<<<<<<<< ';

            TRUNCATE TABLE [bronze].[erp_cust_az12];

            PRINT '>>>>>>>>>>>>>>> Inserting data into: [bronze].[erp_cust_az12] <<<<<<<<<<<<<<<<<< ';

            BULK INSERT [bronze].[erp_cust_az12]
            FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
            WITH
            (
                   FIRSTROW = 2,
                   FIELDTERMINATOR = ',',
                   TABLOCK
            );



            ---------------------------------------------- BULK INSERT for Table bronze.erp_loc_a101 -----------------------------------------


            PRINT '>>>>>>>>>>>>>>> Truncating Table: [bronze].[erp_loc_a101] <<<<<<<<<<<<<<<<<< ';
            TRUNCATE TABLE [bronze].[erp_loc_a101];

            PRINT '>>>>>>>>>>>>>>> Inserting data into: [bronze].[erp_loc_a101] <<<<<<<<<<<<<<<<<< ';
            BULK INSERT [bronze].[erp_loc_a101]
            FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
            WITH
            (
                   FIRSTROW = 2,
                   FIELDTERMINATOR = ',',
                   TABLOCK
            );


            ---------------------------------------------- BULK INSERT for Table bronze.erp_px_cat_g1v2 -----------------------------------------

            PRINT '>>>>>>>>>>>>>>> Truncating Table: [bronze].[erp_px_cat_g1v2] <<<<<<<<<<<<<<<<<< ';
            TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];


            PRINT '>>>>>>>>>>>>>>> Inserting data into: [bronze].[erp_px_cat_g1v2] <<<<<<<<<<<<<<<<<< ';
            BULK INSERT [bronze].[erp_px_cat_g1v2]
            FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
            WITH
            (
                   FIRSTROW = 2,
                   FIELDTERMINATOR = ',',
                   TABLOCK
            );

    END TRY

    BEGIN CATCH

                    PRINT '==============================================================================================================='
                    PRINT '                                         Error Occured During Loading on the Bronze Layer                      '
                    PRINT '                                         The Error was:' + ERROR_MESSAGE();         
                    PRINT '                                         The Error was:' + CAST(ERROR_NUMBER() AS NVARCHAR);
                    PRINT '                                         The Error was:' + CAST(ERROR_STATE() AS NVARCHAR);
                    PRINT '==============================================================================================================='

    END CATCH

END



/*************************************************************************************************************************************************

    STORED PROCEDURE - Track ETL Duration
        
        - Helps to identify how long it took to run the ETL process
        - We need to set the Starting Time and Ending Time
            - Example: Time it takes to load tables

        SYNTAX and Exercise:
        - Create the Starting and Ending of loading each table.
        - Calculate how long it took to calculate each layer.

            - Declare Start Time and End Time

                DECLARE @Start_Time DATETIME
                DECKARE @End_Time DATETIME

            - Set the Variable/ Giving the variable a value
                
                SET @Start_Time = GETDATE()

            - Calculate the difference in seconds, and CAST into NVARCAHR
                
                PRINT 'Difference is: ' + CAST(DATEDIFF(seconds, @Start_Time, @End_Time) as NVARCHAR) + ' seconds'

        Exercise 2:
        - Calculate the Duraction of Loading Bronze Layer


**************************************************************************************************************************************************/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN

DECLARE @Start_Time DATETIME, @End_Time   DATETIME ;       ------ Declaring the Start_Time and End_Time

DECLARE @Batch_Start_Time DATETIME, @Batch_End_Time DATETIME;

    BEGIN TRY
            
            SET @Batch_End_Time = GETDATE();

            PRINT '================================================================================================================================';
            PRINT 'Loading the Bronze Layer'
            PRINT '================================================================================================================================';




            PRINT '---------------------------------------------------------------------------------------------------------------------------------';
            PRINT 'Loading of CRM tables'
            PRINT '---------------------------------------------------------------------------------------------------------------------------------';



            ---------------------------------------------- BULK INSERT for Table bronze.crm_cust_info -----------------------------------------



            SET @Start_Time = GETDATE();  ------ The Start Time of the load
            
            PRINT '>>>>>>>>>>>>>>> Truncating Table: bronze.crm_cust_info <<<<<<<<<<<<<<<<<< ';

            TRUNCATE TABLE bronze.crm_cust_info


            PRINT '>>>>>>>>>>>>>>> Inserting data into: bronze.crm_cust_info <<<<<<<<<<<<<<<<<< ';
            BULK INSERT bronze.crm_cust_info

            FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'

            WITH
            (
                FIRSTROW = 2,           ------------- Telling SQL to data reading data from 2nd Row
                FIELDTERMINATOR = ',',  ------------- SQL File is separated by commas
                TABLOCK                 ------------- Telling SQL to lock the table to this table when importing the data
            );

            SET @End_Time = GETDATE();  -------- The End Time of the Load
            PRINT ' >>>> Load Duration: ' + CAST(DATEDIFF(second, @Start_Time, @End_Time) as NVARCHAR) + ' seconds <<<<';
            PRINT '---------------------------------------------------------------------------------------------------';


----------------------------------------------------------------------------------------------------------------------------------------------------------




            ---------------------------------------------- BULK INSERT for Table bronze.crm_prd_info -----------------------------------------



            SET @Start_Time = GETDATE();

            PRINT '>>>>>>>>>>>>>>> Truncating Table: [bronze].[crm_prd_info] <<<<<<<<<<<<<<<<<< ';

            TRUNCATE TABLE [bronze].[crm_prd_info];

            PRINT '>>>>>>>>>>>>>>> Inserting data into: [bronze].[crm_prd_info] <<<<<<<<<<<<<<<<<< ';

            BULK INSERT [bronze].[crm_prd_info]
            FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
            WITH
            (
                   FIRSTROW = 2,
                   FIELDTERMINATOR = ',',
                   TABLOCK
            );

            SET @End_Time = GETDATE();
            PRINT ' >>>> Load Duration: ' + CAST(DATEDIFF(second, @Start_Time, @End_Time) as NVARCHAR) + ' seconds <<<<';
            PRINT '---------------------------------------------------------------------------------------------------';




            ---------------------------------------------- BULK INSERT for Table bronze.crm._sales_details -----------------------------------------

            SET @Start_Time = GETDATE();

            PRINT '>>>>>>>>>>>>>>> Truncating Table: [bronze].[crm_sales_details] <<<<<<<<<<<<<<<<<< ';

            TRUNCATE TABLE [bronze].[crm_sales_details];

            PRINT '>>>>>>>>>>>>>>> Inserting data into: [bronze].[crm_sales_details] <<<<<<<<<<<<<<<<<< ';

            BULK INSERT [bronze].[crm_sales_details]
            FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
            WITH
            (
                   FIRSTROW = 2,
                   FIELDTERMINATOR = ',',
                   TABLOCK
            );

            SET @End_Time = GETDATE();
            PRINT ' >>>> Load Duration: ' + CAST(DATEDIFF(second, @Start_Time, @End_Time) as NVARCHAR) + ' seconds <<<<';
            PRINT '---------------------------------------------------------------------------------------------------';

--------------------------------------------------------------------------------------------------------------------------------------------------------------






            PRINT '---------------------------------------------------------------------------------------------------------------------------------';
            PRINT 'Loading of ERP tables'
            PRINT '---------------------------------------------------------------------------------------------------------------------------------';


            ---------------------------------------------- BULK INSERT for Table bronze.erp_cust_az12 -----------------------------------------


            SET @Start_Time = GETDATE();

            PRINT '>>>>>>>>>>>>>>> Truncating Table: [bronze].[erp_cust_az12] <<<<<<<<<<<<<<<<<< ';

            TRUNCATE TABLE [bronze].[erp_cust_az12];

            PRINT '>>>>>>>>>>>>>>> Inserting data into: [bronze].[erp_cust_az12] <<<<<<<<<<<<<<<<<< ';

            BULK INSERT [bronze].[erp_cust_az12]
            FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
            WITH
            (
                   FIRSTROW = 2,
                   FIELDTERMINATOR = ',',
                   TABLOCK
            );

            SET @End_Time = GETDATE();
            PRINT ' >>>> Load Duration: ' + CAST(DATEDIFF(second, @Start_Time, @End_Time) as NVARCHAR) + ' seconds <<<<';
            PRINT '---------------------------------------------------------------------------------------------------';


-----------------------------------------------------------------------------------------------------------------------------------------------------





            ---------------------------------------------- BULK INSERT for Table bronze.erp_loc_a101 -----------------------------------------

            SET @Start_Time = GETDATE();

            PRINT '>>>>>>>>>>>>>>> Truncating Table: [bronze].[erp_loc_a101] <<<<<<<<<<<<<<<<<< ';
            TRUNCATE TABLE [bronze].[erp_loc_a101];

            PRINT '>>>>>>>>>>>>>>> Inserting data into: [bronze].[erp_loc_a101] <<<<<<<<<<<<<<<<<< ';
            BULK INSERT [bronze].[erp_loc_a101]
            FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
            WITH
            (
                   FIRSTROW = 2,
                   FIELDTERMINATOR = ',',
                   TABLOCK
            );

            SET @End_Time = GETDATE();
            PRINT ' >>>> Load Duration: ' + CAST(DATEDIFF(second, @Start_Time, @End_Time) as NVARCHAR) + ' seconds <<<<';
            PRINT '---------------------------------------------------------------------------------------------------';


------------------------------------------------------------------------------------------------------------------------------------------------------------



            ---------------------------------------------- BULK INSERT for Table bronze.erp_px_cat_g1v2 ------------------------------------------------

            SET @Start_Time = GETDATE()

            PRINT '>>>>>>>>>>>>>>> Truncating Table: [bronze].[erp_px_cat_g1v2] <<<<<<<<<<<<<<<<<< ';
            TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];


            PRINT '>>>>>>>>>>>>>>> Inserting data into: [bronze].[erp_px_cat_g1v2] <<<<<<<<<<<<<<<<<< ';
            BULK INSERT [bronze].[erp_px_cat_g1v2]
            FROM 'C:\Users\Cash\Desktop\SQL Server - Datawarehouse - Data Engineering\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
            WITH
            (
                   FIRSTROW = 2,
                   FIELDTERMINATOR = ',',
                   TABLOCK
            );

            SET @End_Time = GETDATE();
            PRINT ' >>>> Load Duration: ' + CAST(DATEDIFF(second, @Start_Time, @End_Time) as NVARCHAR) + ' seconds <<<<';
            PRINT '---------------------------------------------------------------------------------------------------';


----------------------------------------------------------------------------------------------------------------------------------------------------------------


            SET @Batch_End_Time = GETDATE();     ----- Getting the End Time of the Loading Layer


            ----------------------------------------- Displaying the duration for Loading the Bronze Layer --------------------------------------------

            PRINT '====================================================================================================================================';
            PRINT 'Loading of the Bronze Later is completed';
            PRINT 'The Duration for the Load of Bronze Layer is: ' + CAST(DATEDIFF(Second, @Batch_Start_Time, @Batch_End_Time) AS NVARCHAR) + ' seconds'
            PRINT '====================================================================================================================================';

    END TRY



    BEGIN CATCH

                    PRINT '===============================================================================================================';
                    PRINT '                                         Error Occured During Loading on the Bronze Layer                      ';
                    PRINT '                                         The Error was:' + ERROR_MESSAGE();         
                    PRINT '                                         The Error was:' + CAST(ERROR_NUMBER() AS NVARCHAR);
                    PRINT '                                         The Error was:' + CAST(ERROR_STATE() AS NVARCHAR);
                    PRINT '===============================================================================================================';

    END CATCH

END
