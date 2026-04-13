


/********************************************************************************************************************************************

	Section 4: Build Bronze Layer

		Importing Data from CSV files into Excel Bronze Layer

********************************************************************************************************************************************/


Use Datawarehouse;   ---- Selecting the database
GO

GO
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
GO
