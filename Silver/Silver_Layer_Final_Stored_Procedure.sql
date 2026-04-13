

/**********************************************************************************************************

	Silver Layer codes

**********************************************************************************************************/


---EXEC [silver].[load_Silver]   ------- Executing the stored procedure to load the silver layer before loading the silver layer

----Use Datawarehouse

CREATE OR ALTER PROCEDURE [Silver].[Load_Silver] AS

BEGIN

DECLARE @Start_Time DATETIME;

DECLARE @End_Time DATETIME;


DECLARE @batch_start_time DATETIME;

DECLARE @batch_end_time DATETIME;




    BEGIN TRY
                

               --- DECLARE @Start_Time DATETIME;

               --- DECLARE @End_Time DATETIME;

                PRINT '================================================================================================================================';
                PRINT 'Loading the Silver Layer'
                PRINT '================================================================================================================================';


                PRINT '---------------------------------------------------------------------------------------------------------------------------------';
                PRINT 'Loading of CRM tables'
                PRINT '---------------------------------------------------------------------------------------------------------------------------------';



                SET @Start_Time = GETDATE();                                            ------ Begining of the insert

                PRINT '>>Truncating table: Datawarehouse.silver.crm_cust_info<<'
                TRUNCATE TABLE  Datawarehouse.silver.crm_cust_info

                PRINT '>> INSERTING DATA INTO: Datawarehouse.silver.crm_cust_info';

                INSERT INTO     Datawarehouse.silver.crm_cust_info
                (
                    cst_id,
                    cst_key,
                    cst_firstname,
                    cst_lastname,
                    cst_marital_status,
                    cst_gndr,
                    cst_create_date
                )

                SELECT      x.cst_id,
                            x.cst_key,
                            [cst_firstname] = TRIM(X.cst_firstname),
                            [cst_lastname] = TRIM(X.cst_lastname),
                            [X.cst_marital_status] = 
                            CASE
                                WHEN  UPPER(TRIM(X.cst_marital_status)) = 'S' THEN 'Single'
                                WHEN  UPPER(TRIM(X.cst_marital_status)) = 'M' THEN 'Married'
                                ELSE    
                                      'n/a'
                            END,
                            [X.cst_gndr] =  
                            CASE
                                WHEN UPPER(TRIM(X.cst_gndr)) = 'F' THEN 'Female'
                                WHEN UPPER(TRIM(X.cst_gndr)) = 'M' THEN 'Male'
                                ELSE
                                    'n/a'
                            END,
                            cst_create_date
                FROM
                (
                    SELECT  cst_id,
                            cst_key,
                            cst_firstname,
                            [First_Name Space Check] = LEN(cst_firstname) - len(trim(cst_firstname)),   ------ Checking for space on First Name
                            cst_lastname,
                            [Last_Name Space Check] = LEN(cst_lastname) - len(trim(cst_lastname)),      ------ Checking for space on Last Name
                            cst_marital_status,
                            cst_gndr,
                            cst_create_date,
                            [Last Inserted Record] = DENSE_RANK() OVER(PARTITION BY cst_id order by cst_create_date DESC)
    
                    FROM    Datawarehouse.[bronze].[crm_cust_info]

                    WHERE   cst_id IS NOT NULL
                )
                X
                WHERE X.[Last Inserted Record] = 1


                  SET @End_Time = GETDATE();  -------- The End Time of the Insert
                  PRINT ' >>>> Insert Duration for table Datawarehouse.silver.crm_cust_info: ' + CAST(DATEDIFF(second, @Start_Time, @End_Time) as NVARCHAR) + ' seconds <<<<';
                  PRINT '---------------------------------------------------------------------------------------------------';





                ----------------------------------------------- Inserting data into table 'Datawarehouse.silver.crm_prd_info'


                SET @Start_Time = GETDATE();

                PRINT '>>Truncating table: Datawarehouse.silver.crm_prd_info<<'
                TRUNCATE TABLE  [Datawarehouse].[silver].[crm_prd_info]

                PRINT 'INSERTING DATA INTO TABLE: Datawarehouse.silver.crm_prd_info'
                INSERT INTO [Datawarehouse].[silver].[crm_prd_info] 
                (
                    prd_id          ,
                    cat_id          ,
                    prd_key         ,
                    prd_nm          ,
                    prd_cost        ,
                    prd_line        ,
                    prd_start_dt    ,
                    prd_end_dt      
                )


                SELECT            X.prd_id,
                                  [cat_id] = x.Cat_id_fk,
                                  [prd_key] = x.prd_key_fk,
                                  x.prd_nm,
                                  x.prd_cost,
                                  x.prd_line,
                                  [prd_start_dt] = CAST(x.prd_start_dt as DATE),
                                  [prd_end_dt] = CAST(x.[My clean end date] as DATE)


                FROM

                (

                        SELECT      prd_id,
                                    prd_key,
                                    [Cat_id_fk] = Replace(SUBSTRING(prd_key, 1, 5), '-', '_'), ----- Created column to link with column id on table [bronze].[erp_px_cat_g1v2]
                                    [prd_key_fk] = SUBSTRING(prd_key, 7, LEN(prd_key)),        ----- Created column to link with column prd_key on table  [bronze].[crm_sales_details]
                                    prd_nm,
                                    [prd_cost] = ISNULL(prd_cost, 0),
                                    prd_line =                                                 ---- Standardization and Consitency is taking place here -----
                                    CASE
                                        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                                        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                                        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                                        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                                        ELSE
                                            'n/a'
                                    END,                                                        ---- It ending here
                                    prd_start_dt,
                                    prd_end_dt, 
                                    [My clean end date] = 
                                    CASE
                                            WHEN prd_start_dt > prd_end_dt THEN (Lead(prd_start_dt, 1) OVER(PARTITION by prd_key ORDER by prd_start_dt)) - 1
                                            WHEN prd_end_dt > prd_start_dt THEN prd_end_dt
                                    END

                        FROM        Datawarehouse.bronze.crm_prd_info

                )
                X

                  SET @End_Time = GETDATE();  -------- The End Time of the Insert
                  PRINT ' >>>> Insert Duration for table [Datawarehouse].[silver].[crm_prd_info]: ' + CAST(DATEDIFF(second, @Start_Time, @End_Time) as NVARCHAR) + ' seconds <<<<';
                  PRINT '---------------------------------------------------------------------------------------------------';








                ---------------- INSERTING DATA INTO TABLE [Datawarehouse].[silver].[crm_sales_details]


                 SET @Start_Time = GETDATE();



                PRINT '>>Truncating table: [Datawarehouse].[silver].[crm_sales_details]<<'
                TRUNCATE TABLE  [Datawarehouse].[silver].[crm_sales_details]

                PRINT 'INSERTING DATA INTO TABLE: [Datawarehouse].[silver].[crm_sales_details]'

                IF OBJECT_ID('[Datawarehouse].[silver].[crm_sales_details]', 'U') IS NOT NULL
                        DROP TABLE [Datawarehouse].[silver].[crm_sales_details]
                CREATE TABLE [Datawarehouse].[silver].[crm_sales_details]
                (
                            sls_ord_num NVARCHAR (50),
                            sls_prd_key NVARCHAR (50),
                            sls_cust_id INT,
                            sls_order_dt DATE,
                            sls_ship_dt DATE,
                            sls_due_dt DATE,
                            [sls_sales] INT,
                            sls_quantity INT,
                            sls_price INT,
                            dwh_create_date DATETIME2 DEFAULT GETDATE()
                );


                INSERT INTO [Datawarehouse].[silver].[crm_sales_details]
                (
                            sls_ord_num ,
                            sls_prd_key ,
                            sls_cust_id ,
                            sls_order_dt ,
                            sls_ship_dt ,
                            sls_due_dt ,
                            [sls_sales] ,
                            sls_quantity ,
                            sls_price 
                )

                SELECT      X.sls_ord_num,
                            X.sls_prd_key,
                            X.sls_cust_id,
                            X.sls_order_dt,
                            X.sls_ship_dt,
                            X.sls_due_dt,
                            X.sls_sales,
                            X.sls_quantity,
                            X.sls_price

                FROM
                (
                    SELECT  sls_ord_num,
                            sls_prd_key,
                            sls_cust_id,
                            [sls_order_dt] = 
                            CASE
                                WHEN LEN(sls_order_dt) = 0 OR LEN(sls_order_dt) <> 8 THEN NULL              --- Addressing the issue of data Length in column sls_order_dt
                                ELSE
                                    CAST(CAST(sls_order_dt as varchar) as date)
                            END,
                            sls_ship_dt = 
                            CASE
                                WHEN LEN(sls_ship_dt) = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL                --- Addressing the issue of data length in column sls_ship_dt
                                ELSE
                                    CAST(CAST(sls_ship_dt as varchar) as date)
                            END,
                            sls_due_dt = 
                            CASE
                                WHEN LEN(sls_due_dt) = 0 OR LEN(sls_due_dt) <> 8 THEN NULL                  --- Addressing the issue of data length in column sls_due_dt
                                ELSE
                                    CAST(CAST(sls_due_dt as varchar) as date)
                            END,
                            [sls_sales] = ISNULL(ABS(sls_quantity * sls_price),0),
                            [sls_quantity] = ISNULL(ABS(sls_quantity),0),
                            [sls_price] = ISNULL(ABS(sls_price),0)

                    FROM    Datawarehouse.bronze.crm_sales_details

                    GROUP BY sls_ord_num,
                            sls_prd_key,
                            sls_cust_id,
                            sls_order_dt,
                            sls_ship_dt,
                            sls_due_dt,
                            [sls_sales],
                            sls_quantity,
                            sls_price
                )
                X


                  SET @End_Time = GETDATE();  -------- The End Time of the Insert
                  PRINT ' >>>> Insert Duration for table [Datawarehouse].[silver].[crm_sales_details]: ' + CAST(DATEDIFF(second, @Start_Time, @End_Time) as NVARCHAR) + ' seconds <<<<';
                  PRINT '---------------------------------------------------------------------------------------------------';






                PRINT '---------------------------------------------------------------------------------------------------------------------------------';
                PRINT 'Loading of ERP tables'
                PRINT '---------------------------------------------------------------------------------------------------------------------------------';

                
                ----------- INSERTING DATA INTO TABLE: [Datawarehouse].[silver].[erp_cust_az12]


                SET @Start_Time = GETDATE();


                PRINT '>>Truncating table: [Datawarehouse].[silver].[erp_cust_az12]<<'
                TRUNCATE TABLE  [Datawarehouse].[silver].[erp_cust_az12]

                Print 'INSERTING DATA INTO TABLE:  [Datawarehouse].[silver].[erp_cust_az12]'
                INSERT INTO [Datawarehouse].[silver].[erp_cust_az12]
                (
                    cid ,
                    bdate ,
                    gen
                )


                SELECT      cid,
                            bdate,
                            gen

                FROM     
                (
                            SELECT      cid,
                                        bdate = 
                                        CASE
                                            WHEN bdate > GETDATE() THEN NULL               --- Date problem issue Addressed
                                            ELSE
                                                    bdate
                                        END,
                                        gen = 
                                        CASE
                                            WHEN TRIM(gen) IS NULL THEN 'n/a'
                                            WHEN TRIM(gen) = '' THEN 'n/a'                  --- Data Inconsistency Addressed
                                            WHEN TRIM(gen) = 'F' THEN 'Female'
                                            WHEN TRIM(gen) = 'M' THEN 'Male'
                                            ELSE
                                                gen
                                        END

                            FROM    Datawarehouse.[bronze].[erp_cust_az12]
                )
                X

                SET @End_Time = GETDATE()

                SET @End_Time = GETDATE();  -------- The End Time of the Insert
                PRINT ' >>>> Insert Duration for table [Datawarehouse].[silver].[erp_cust_az12]: ' + CAST(DATEDIFF(second, @Start_Time, @End_Time) as NVARCHAR) + ' seconds <<<<';
                PRINT '---------------------------------------------------------------------------------------------------';




                ---------- INSERTING DATA INTO TABLE [Datawarehouse].[silver].[erp_px_cat_g1v2]


                SET @Start_Time = GETDATE();


                PRINT '>>Truncating table: [Datawarehouse].[silver].[erp_px_cat_g1v2]<<'
                TRUNCATE TABLE  [Datawarehouse].[silver].[erp_px_cat_g1v2]


                PRINT 'INSERTING DATA INTO TABLE [Datawarehouse].[bronze].[erp_loc_a101]:'
                INSERT INTO [Datawarehouse].[silver].[erp_px_cat_g1v2] (id, cat, subcat, maintenance)

                SELECT      id,
                            cat,
                            subcat,
                            maintenance

                FROM        Datawarehouse.bronze.erp_px_cat_g1v2



                SET @End_Time = GETDATE();  -------- The End Time of the Insert
                PRINT ' >>>> Insert Duration for table [Datawarehouse].[Silver].[erp_px_cat_g1v2]: ' + CAST(DATEDIFF(second, @Start_Time, @End_Time) as NVARCHAR) + ' seconds <<<<';
                PRINT '---------------------------------------------------------------------------------------------------';



                --------- INSERTING DATA INTO TABLE Datawarehouse].[Silver].[erp_loc_a101]:  


                SET @Start_Time = GETDATE()

                PRINT '>>Truncating table: [Datawarehouse].[Silver].[erp_loc_a101]<<'
                TRUNCATE TABLE  [Datawarehouse].[Silver].[erp_loc_a101]

                PRINT 'INSERTING DATA INTO TABLE: [Datawarehouse].[Silver].[erp_loc_a101]'
                INSERT INTO [Datawarehouse].[Silver].[erp_loc_a101]
                (
                    cid,
                    cntry
                )

                SELECT      X.cid,
                            X.cntry

                FROM
                (
                    SELECT      cid = CAST(replace(cid, '-','') as nvarchar(50)),
                                cntry = 
                                CASE
                                    WHEN cntry = '' OR cntry IS NULL THEN 'n/a'
                                    WHEN TRIM(UPPER(cntry)) = 'DE' THEN 'Germany'                           ----- Data Consistency Addressed
                                    WHEN cntry in ('united States', 'US', 'USA') THEN 'United States'       
                                    Else                                                                        
                                        cntry
                                END

                    FROM        Datawarehouse.[bronze].[erp_loc_a101]
                )
                X

                SET @End_Time = GETDATE();  -------- The End Time of the Insert
                PRINT ' >>>> Insert Duration for table [Datawarehouse].[Silver].[erp_loc_a101]: ' + CAST(DATEDIFF(second, @Start_Time, @End_Time) as NVARCHAR) + ' seconds <<<<';
                PRINT '--------------------------------------------------------------------------------------------------------------------------------------------------------';

                SET @batch_start_time = GETDATE();
                PRINT '========================================================================================================================================================'
                PRINT '                                         The Silver Layer has been loaded successfully!                      ';
                PRINT '                                         Total Load Duration: ' +CAST(DATEDIFF(second, @batch_start_time, GETDATE()) as NVARCHAR) + ' seconds';

    END TRY

    BEGIN CATCH 
                    PRINT '===============================================================================================================';
                    PRINT '                                         Error Occured During Loading on the Silver Layer                      ';
                    PRINT '                                         The Error was:' + ERROR_MESSAGE();         
                    PRINT '                                         The Error was:' + CAST(ERROR_NUMBER() AS NVARCHAR);
                    PRINT '                                         The Error was:' + CAST(ERROR_STATE() AS NVARCHAR);
                    PRINT '===============================================================================================================';
    END CATCH 


END
