
/***********************************************************************************************************************************************

	22. Exploring Data:
		- Analyzing tables from the Bronze Layer

		- Here we explore Data in order to Design the ERD on drawio

***********************************************************************************************************************************************/
	 

------------------------------------------------------------------------- CRM Source ------------------------------------------------------------


	 SELECT *	--[customer Name] = cst_firstname,
				--[length] = LEN(cst_firstname) - LEN(trim(cst_firstname))
	  FROM		Datawarehouse.bronze.crm_cust_info
	

	  SELECT    *
	  FROM      Datawarehouse.bronze.crm_prd_info
 

	  SELECT    *
	  FROM      Datawarehouse.bronze.crm_sales_details



-------------------------------------------------------------- ERP Source ---------------------------------------------------------------


	 SELECT *	--[customer Name] = cst_firstname,
				--[length] = LEN(cst_firstname) - LEN(trim(cst_firstname))

	 FROM		Datawarehouse.bronze.erp_cust_az12


	 SELECT    *
	 FROM      Datawarehouse.bronze.erp_loc_a101


	 SELECT		*
	 FROM		Datawarehouse.bronze.erp_px_cat_g1v2
  


/***********************************************************************************************************************************************

	23. DDL Create Tables

***********************************************************************************************************************************************/
	 
	 USE Datawarehouse

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()              ----- Date and Time when data will be added into this column every time a new record will be added
);



IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()              ----- Date and Time when data will be added into this column every time a new record will be added
);
GO



IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()              ----- Date and Time when data will be added into this column every time a new record will be added
);
GO



IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()              ----- Date and Time when data will be added into this column every time a new record will be added
);
GO



IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()              ----- Date and Time when data will be added into this column every time a new record will be added
);
GO



IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
    dwh_create_date     DATETIME2 DEFAULT GETDATE()              ----- Date and Time when data will be added into this column every time a new record will be added
);
GO



/***********************************************************************************************************************************************

	23. LOAD SCRIP 1 - Data Cleaning
        
            - Checking data from the BRONZE LAYER, cleaning the Data then inserting the data into the SILVER LAYER

            CheckList for Data Quality are:

                - Primary Key duplication.
                       - Column cst_id, cst_key

                - Checking for speaces before or after characters.
                       - column cst_firstname, cst_lastname

                - Data Standardization & Consistency
                        - Checking for distinct values within a column
                        - column cst_marital_status, cst_gndr

***********************************************************************************************************************************************/



/***********************************************************************************************************************************************

	Cleaning the data from table [bronze].[crm_cust_info]

***********************************************************************************************************************************************/




---------------------------------------- Checking for duplications in Primary Key (BRONZE) --------------------------------------------

SELECT  cst_id,
        COUNT(*)

FROM        Datawarehouse.bronze.crm_cust_info

GROUP By    cst_id

HAVING count(*) > 1 OR cst_id IS NULL


------------------------------------------- Clean code after checking of Primary Key (PK) ------------------------------------------------------------



SELECT      *

FROM
(
    SELECT  cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date,
            [Last Inserted Record] = DENSE_RANK() OVER(PARTITION BY cst_id order by cst_create_date DESC) ----- Checking for the last inserted record on cst_create_date
    
    FROM    Datawarehouse.[bronze].[crm_cust_info]
)
X
WHERE X.[Last Inserted Record] = 1  ----- Only filtering those records which have duplicates





---------------------------------------- Checking for speaces before or after characters (BRONZE) --------------------------------------------


SELECT      [First_Name Space Check] = LEN(cst_firstname) - len(trim(cst_firstname)),   ------ Checking for space on First Name
            [Last_Name Space Check] = LEN(cst_lastname) - len(trim(cst_lastname))      ------ Checking for space on Last Name


FROM        Datawarehouse.bronze.crm_cust_info


--------------------------------------------------- Clean Code After Space Check -------------------------------------------------------

SELECT      *

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
)
X

WHERE X.[Last Inserted Record] = 1




---------------------------------------- Data Standardization & Consistency (BRONZE) --------------------------------------------

SELECT distinct cst_gndr

FROM   Datawarehouse.bronze.crm_cust_info


SELECT distinct cst_marital_status

FROM   Datawarehouse.bronze.crm_cust_info


---------------------------------- The final clean load for Datawarehouse.[bronze].[crm_cust_info] --------------------


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





/**************************************************************************************************************************************
    
    Inserting clean data into table [silver].[crm_prd_info]

**************************************************************************************************************************************/

/*
SELECT  *
    
FROM    Datawarehouse.silver.crm_cust_info
*/


INSERT INTO     silver.crm_cust_info
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



/**************************************************************************************************************************************
    
    Checking if Data was cleanely loaded in table [silver].[crm_prd_info]

**************************************************************************************************************************************/


-------------------------------- Check if the records were sucessfully inserted -------------------------------------------------------


SELECT  *
    
FROM    Datawarehouse.silver.crm_cust_info



---------------------------------------- Checking for duplications in Primary Key (SILVER) --------------------------------------------


SELECT  cst_id,
        COUNT(*)

FROM        Datawarehouse.silver.crm_cust_info

GROUP By    cst_id

HAVING count(*) > 1 OR cst_id IS NULL


---------------------------------------- Checking for speaces before or after characters (SILVER) --------------------------------------------

SELECT *

FROM
(
    SELECT      [First_Name Space Check] = LEN(cst_firstname) - len(trim(cst_firstname)),   ------ Checking for space on First Name
                [Last_Name Space Check] = LEN(cst_lastname) - len(trim(cst_lastname))      ------ Checking for space on Last Name


    FROM        Datawarehouse.silver.crm_cust_info
)
X

WHERE X.[First_Name Space Check] > 0 OR X.[Last_Name Space Check] > 0


---------------------------------------- Data Standardization & Consistency (SILVER) --------------------------------------------

SELECT distinct cst_gndr

FROM   Datawarehouse.silver.crm_cust_info


SELECT distinct cst_marital_status

FROM   Datawarehouse.silver.crm_cust_info




/**************************************************************************************************************************************
    
    25. LOAD SCRIPT 2
          
           - Cleaning the Data from table [bronze].[crm_prd_info]

**************************************************************************************************************************************/



SELECT  *

FROM    Datawarehouse.[bronze].[crm_prd_info]



-------- Checking Primary Key prd_id -------

SELECT      prd_id,
            [Number of times it occured] = 
            count(*)

FROM        Datawarehouse.[bronze].[crm_prd_info]

GROUP BY    prd_id

HAVING      count(*) > 1


---------  Getting the Category ID from column prd_key from table bronze.crm_prd_info, then match the format with column id from table  bronze.erp_px_cat_g1v2  ------------

SELECT      prd_id,
            prd_key,
            [Cat_id] = Replace(SUBSTRING(prd_key, 1, 5), '-', '_'), ----- Created column to link with column id on table [bronze].[erp_px_cat_g1v2]
            [prd_key] = SUBSTRING(prd_key, 7, LEN(prd_key)),        ----- Created column to link with column prd_key on table  [bronze].[crm_sales_details]
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt

FROM        Datawarehouse.bronze.crm_prd_info



---- Checking for duplication of PK on column prd_id in table bronze.crm_prd_info 

SELECT      prd_id,
            count(*)

FROM        Datawarehouse.bronze.crm_prd_info

GROUP BY    prd_id

HAVING  COUNT(*) > 1


---- Checking for Unwanted Space on column prd_nm on table bronze.crm_prd_info 


SELECT *

FROM
(
        SELECT      prd_nm,
                    [Trimed Column] = TRIM(prd_nm),
                    [Length of Spaces] = LEN(prd_nm) - LEN(TRIM(prd_nm))

        FROM        Datawarehouse.bronze.crm_prd_info
)
X
WHERE X.prd_nm != x.[Trimed Column]



---- Checking for Quality of Numbers to see if we have NULL's or Negative numbers on column prd_cost in table [bronze].[crm_prd_info]

SELECT      prd_cost

FROM        Datawarehouse.bronze.crm_prd_info

WHERE       prd_cost IS NULL OR prd_cost < 0




--------------------------- Cleaned code addressing Standardization & Consistency --------------------------


SELECT            X.prd_id,
                  x.prd_key,
                  x.Cat_id_fk,
                  x.prd_key_fk,
                  x.prd_nm,
                  x.prd_cost,
                  x.prd_line,
                  x.prd_start_dt,
                  x.prd_end_dt


FROM

(

        SELECT      prd_id,
                    prd_key,
                    [Cat_id_fk] = Replace(SUBSTRING(prd_key, 1, 5), '-', '_'), ----- Created column to link with column id on table [bronze].[erp_px_cat_g1v2]
                    [prd_key_fk] = SUBSTRING(prd_key, 7, LEN(prd_key)),        ----- Created column to link with column prd_key on table  [bronze].[crm_sales_details]
                    prd_nm,
                    [prd_cost] = ISNULL(prd_cost, 0),
                    prd_line = 
                    CASE
                        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                        ELSE
                            'n/a'
                    END,
                    prd_start_dt,
                    prd_end_dt

        FROM        Datawarehouse.bronze.crm_prd_info

)
X








----- Data Standardization & Consistency for column prd_line in table  bronze.crm_prd_info ------

SELECT      distinct prd_line

FROM        Datawarehouse.bronze.crm_prd_info




SELECT            X.prd_id,
                  x.prd_key,
                  x.Cat_id_fk,
                  x.prd_key_fk,
                  x.prd_nm,
                  x.prd_cost,
                  x.prd_line,
                  x.prd_start_dt,
                  x.prd_end_dt


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
                    prd_end_dt

        FROM        Datawarehouse.bronze.crm_prd_info

)
X





------ Check for Invalid Date Orders
------ Checking for End Dates that are smaller than Start Dates. This is because the End Date does not have to be before the Starting date
------ Here we are taking Start Date of the same product Key, then looking to the next starting date on the same column minus (-) 1

SELECT      *,
            [Next Starting Date based on Product ID] = (Lead(prd_start_dt, 1) OVER(PARTITION by prd_key ORDER by prd_start_dt)) - 1,
            [Logic] = 
            CASE
                WHEN prd_start_dt > prd_end_dt THEN (Lead(prd_start_dt, 1) OVER(PARTITION by prd_key ORDER by prd_start_dt)) - 1
                WHEN prd_end_dt > prd_start_dt THEN prd_end_dt
            END

FROM        Datawarehouse.bronze.crm_prd_info

WHERE       prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509') ---- Only checking for these Product_Key, in order to test our logic


------------------------------------- Clean code Addressing invalid Date Orders -----------------

SELECT            X.prd_id,
                  x.prd_key,
                  x.Cat_id_fk,
                  x.prd_key_fk,
                  x.prd_nm,
                  x.prd_cost,
                  x.prd_line,
                  [prd_start_date] = CAST(x.prd_start_dt as DATE),
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

------------------------------------------------- Inserting the clean data into table silver.crm_prd_info -------------------------------------------------------
------- First we need to make sure that all columns on the Destitation table is the same as in the Source table -----------------------------------------
-------- So, we going to create the table

SELECT *

FROM    Datawarehouse.silver.crm_prd_info



/* Changing the columns in table silver.crm_prd_info because there are new columns that are created after creating the table  

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info
CREATE TABLE silver.crm_prd_info 
(
    prd_id          INT,
    cat_id          NVARCHAR(50),
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
)

*/


INSERT INTO silver.crm_prd_info 
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


------- All data check from table 

SELECT      *

FROM        Datawarehouse.silver.crm_cust_info



SELECT      count(*)
FROM        Datawarehouse.silver.crm_prd_info





/**************************************************************************************************************************************
    
    25. LOAD SCRIPT 3
          
           - Cleaning the Data from table [bronze].[crm_sales_details]

**************************************************************************************************************************************/



----- Checking spaces 
SELECT  *

FROM    Datawarehouse.bronze.crm_sales_details

WHERE   sls_ord_num != TRIM(sls_ord_num)



-----  Creating columns for FK -----
----- This Table is connected on the ERD with table bronze.crm_cust_info


SELECT      *

FROM        Datawarehouse.bronze.crm_sales_details

WHERE       sls_prd_key NOT IN                              ----- Check PK from [bronze.crm_sales_details] if it different with PK in table [silver.crm_prd_info]
(                                                           ----- We can see that none is different. They match
            SELECT      prd_key

FROM        Datawarehouse.silver.crm_prd_info
)



SELECT      *

FROM        Datawarehouse.bronze.crm_sales_details

WHERE       sls_cust_id NOT IN                             ----- Check PK from [bronze.crm_sales_details] if it different with PK in table [silver.crm_cust_info]
(                                                           ----- We can see that none is different. They match
            SELECT      cst_id

FROM        Datawarehouse.silver.crm_cust_info
)




------ Cleaning the date
------ Changing the Data Type from columns the following columns into date: sls_order_dt, sls_ship_dt, sls_due_dt
------ NB: the columns are in integer. They can't be less than 0 before conversion

SELECT      sls_order_dt

FROM        Datawarehouse.bronze.crm_sales_details

WHERE       sls_order_dt < 0



SELECT      [Replacing 0's to NULL] = NULLIF(sls_order_dt,0)

FROM        Datawarehouse.bronze.crm_sales_details

WHERE       sls_order_dt <= 0 
            OR LEN(sls_order_dt) != 8 ----- All numbers should be 8 characters long. So here we checking if there is any number that is no 8 characters on column sls_order_dt




----------------- Cleaner Code -------------------

SELECT         
                MAX(len(sls_order_dt))

FROM            Datawarehouse.bronze.crm_sales_details


SELECT          x.sls_ord_num,
                x.sls_prd_key,
                x.sls_cust_id,
                [sls_order_dt] = x.[sls_order_dt],
                x.sls_ship_dt,
                x.sls_due_dt,
                x.sls_sales,
                x.sls_quantity,
                x.sls_price
FROM
(
    SELECT      sls_ord_num,
                sls_prd_key,
                sls_cust_id,
                sls_order_dt = 
                CASE
                    WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                    ELSE
                           CAST( CAST(sls_order_dt as varchar) as DATE)
                END,
                sls_ship_dt = 
                CASE
                    WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
                    ELSE
                           CAST(CAST(sls_ship_dt as varchar) as DATE)
                END,
                sls_due_dt = 
                CASE
                    WHEN sls_due_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
                    ELSE
                        CAST( CAST(sls_due_dt as varchar) AS DATE)
                END,
                sls_sales,
                sls_quantity,
                sls_price

    FROM        Datawarehouse.bronze.crm_sales_details
)
X



----- Date Check
----- The order Date should always be smaller than the Ship_date and Due_date
----- The result here is GOOD

SELECT      sls_order_dt,
            sls_ship_dt,
            sls_due_dt

FROM        Datawarehouse.[bronze].[crm_sales_details]

WHERE       sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt



/**************************************************************************************************************************************************
       
       Check column sls_quantity:
        - Numbers must not be less than 0, not NULL
        - Business Rule says: Sales = Quantity * Price

***************************************************************************************************************************************************/

SELECT      *,
            [My Sales] = ABS(sls_sales),
            [Sales] = ABS(sls_quantity * sls_price)

FROM        Datawarehouse.bronze.crm_sales_details

WHERE sls_sales < 0 OR sls_sales <> sls_quantity * sls_price OR sls_sales IS NULL

GROUP BY sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_due_dt, sls_due_dt, sls_sales, sls_quantity, sls_price, sls_ship_dt

HAVING      ABS(sls_sales) <> ABS(sls_quantity * sls_price)


---- Addressing the issue in columns: sls_sales, sls_quantity, sls_price

SELECT      *

FROM
(
    SELECT  sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            [sls_order_dt] 
            sls_ship_dt, 
            sls_due_dt,
            [sls_sales] = ISNULL(ABS(sls_quantity * sls_price),0),            ------ Addressing the issue of negative number, and correct formula.
            [sls_quantity] = ABS(sls_quantity),                     ------ Addressing the issue of negative numbers
            [sls_price] = ISNULL(ABS(sls_price),0)                            ------ Addressing the issue of negative numbers

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

WHERE X.sls_sales <= 0 OR X.sls_quantity <= 0 OR sls_price <= 0 OR X.sls_sales IS NULL OR X.sls_quantity IS NULL OR sls_price IS NULL 


/***************************************************************************************************************************************************************
    
    Check columns sls_order_num:
        - Checking if data column [sls_order_num] on table [bronze.crm_sales_details] matches with data in column [sls_cust_id] on table [silver.crm_cust_info]

****************************************************************************************************************************************************************/


SELECT  *

FROM    Datawarehouse.bronze.crm_sales_details

WHERE sls_cust_id NOT IN 
(
    SELECT  sls_cust_id

    FROM    Datawarehouse.silver.crm_cust_info
)

/**************************************************************************************************************************************************************

    Check column sls_prd_num:
        - Checking if data column sls_prd_num on table bronze.crm_sales_details matches with data in column sls_prd_id on table silver.crm_prd_info

*****************************************************************************************************************************************************************/

SELECT  *

FROM    Datawarehouse.bronze.crm_sales_details

WHERE sls_prd_key NOT IN 
(
    SELECT  prd_key

    FROM    Datawarehouse.silver.crm_prd_info
)


/*****************************************************************************************************************************************************************
        
        columns: sls_order_dt, sls_ship_dt, sls_due_dt
              
              Length character check:
                - Checking to see if all dates columns are exactly 8 characters
              
              Checking for Invalid Dates on columns:
                - Checking to see if sls_order_dt > sls_ship_dt. Because orders_date cant be bigger than shipping_date
                - Checking to see if sls_ship_dt > sls_due_dt. Because shipping_date cant be bigger than Due_date
                
******************************************************************************************************************************************************************/

SELECT      sls_order_dt,
            sls_ship_dt,
            [Maximum Length for characters] = MAX(LEN(sls_order_dt)) 

FROM        Datawarehouse.bronze.crm_sales_details

WHERE       sls_order_dt > sls_ship_dt

GROUP BY    sls_order_dt, sls_ship_dt

HAVING      LEN(sls_order_dt) = 0 OR LEN(sls_order_dt) <> 8




SELECT      *

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

WHERE  X.sls_order_dt > sls_ship_dt                                                    --- Checking if Order_date is bigger than Shipping_date. This is correct because Order_date is before the Shipping_date.
       OR X.sls_ship_dt > X.sls_due_dt                                                 --- Checking if Shipping_date is bigger than Order_date. This is also correct because Shipping_Date is before Due_date.



      


/******************************************************************************************************************************************************

        The final implementation.

            - Create table [silver].[crm_sales_details] and Insert the clean from the query Datawarehouse.bronze.crm_sales_details

******************************************************************************************************************************************************/

IF OBJECT_ID('[silver].[crm_sales_details]', 'U') IS NOT NULL
        DROP TABLE [silver].[crm_sales_details]
CREATE TABLE [silver].[crm_sales_details]
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


INSERT INTO silver.crm_sales_details
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



---- Checking table Silver.crm_sales_details

SELECT      *

FROM        Datawarehouse.silver.crm_sales_details


/************************************************************************************************************************************************************************

        NB: After successfully inserting the data into the new table Datawarehouse.silver.crm_sales_details, please run the same check as you did before on each column.
            This time it has to be on the new table Datawarehouse.silver.crm_sales_details.

************************************************************************************************************************************************************************/




/*********************************************************************************************************************************************************

        25. LOAD SCRIPT 3
          
           - Cleaning the Data from table [bronze].[erp_cust_az12]

**********************************************************************************************************************************************************/

SELECT      *

FROM        Datawarehouse.[bronze].[erp_cust_az12]


/****************************************************************************************************************************************

    column cid
        
        - Checking if we can connect it to column id in table [bronze].[crm_prd_info]

****************************************************************************************************************************************/


SELECT  *

FROM    Datawarehouse.[bronze].[erp_cust_az12]
WHERE cid like '%AW00011000'


    SELECT *

    FROM    Datawarehouse.[bronze].[crm_prd_info]
    WHERE prd_key like '%AW00011000'



--------------- Checking if all data in column cid from table Datawarehouse.[bronze].[erp_cust_az12] matches with Datawarehouse.[bronze].[crm_prd_info]

SELECT  *
        
FROM
(
    SELECT  [cid] = 
            CASE
                   WHEN cid like 'NAS%' THEN SUBSTRING(cid, CHARINDEX('AW', cid), LEN(cid))    
                   ELSE
                        cid
            END
            
    FROM    Datawarehouse.[bronze].[erp_cust_az12]
)
X

WHERE X.cid NOT IN 
(
    SELECT  cst_key

    FROM    Datawarehouse.bronze.crm_cust_info
)


--- Or

SELECT  *

FROM    Datawarehouse.[bronze].[erp_cust_az12]

WHERE   cid like 'AW%' AND cid NOT IN
(
    SELECT  cst_key

    FROM    Datawarehouse.bronze.crm_cust_info
)



/**************************************************************************************************************************************

    column bdate:
        
         - Check date range
            - See if customer is older than 1924-01-01
            - See if customer has is born in the future

***************************************************************************************************************************************/

SELECT  bdate,
        [The oldest person] = MIN(bdate) OVER(),
        [The youngest person] = Max(bdate) OVER() ---- Red flag because a person can never be borned in the future

FROM    Datawarehouse.[bronze].[erp_cust_az12]

WHERE   bdate < '1924-01-01' OR bdate > GETDATE()

GROUP BY bdate

ORDER BY bdate DESC


--- Addressing the issue using Case statement

SELECT      *

FROM     
(
            SELECT      cid,
                        bdate = 
                        CASE
                            WHEN bdate > GETDATE() THEN NULL
                            ELSE
                                    bdate
                        END,
                        gen

            FROM    Datawarehouse.[bronze].[erp_cust_az12]
)
X


/**************************************************************************************************************************************
    
    column gen:

        - To check for Data Consistency

***************************************************************************************************************************************/

SELECT  distinct gen,
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


---- Addressing the issue of Data consistency


SELECT      *

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




/*********************************************************************************************************************************

    Final query for table [silver].[erp_cust_az12]
        - Inserting the cleaned data into table [silver].[erp_cust_az12]

**********************************************************************************************************************************/


SELECT      *

FROM        Datawarehouse.[bronze].[erp_cust_az12]


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



/************************************************************************************************************************************

    Checking if the data from the query was succesfully imported into table [Silver].[erp_cust_az12]

    NB: Dont forget to performs all checks from table [bronze].[erp_cust_az12] into table [Silver].[erp_cust_az12]

*************************************************************************************************************************************/


---Checking if all data in column cid from table Datawarehouse.[bronze].[erp_cust_az12] matches with Datawarehouse.[bronze].[crm_prd_info]

SELECT  *
        
FROM
(
    SELECT  [cid] = 
            CASE
                   WHEN cid like 'NAS%' THEN SUBSTRING(cid, CHARINDEX('AW', cid), LEN(cid))    
                   ELSE
                        cid
            END
            
    FROM    Datawarehouse.silver.[erp_cust_az12]
)
X

WHERE X.cid NOT IN 
(
    SELECT  cst_key

    FROM    Datawarehouse.silver.crm_cust_info
)


--- Or

SELECT  *

FROM    Datawarehouse.silver.[erp_cust_az12]

WHERE   cid like 'AW%' AND cid NOT IN
(
    SELECT  cst_key

    FROM    Datawarehouse.silver.crm_cust_info
)


------ Check 1: Checking bdate date ranges

SELECT  bdate,
        [The oldest person] = MIN(bdate) OVER(),
        [The youngest person] = Max(bdate) OVER() ---- Red flag because a person can never be borned in the future

FROM    Datawarehouse.silver.[erp_cust_az12]

WHERE   bdate < '1924-01-01' OR bdate > GETDATE()

GROUP BY bdate

ORDER BY bdate DESC



------ Check 2: Data consistency on column gen. 

SELECT      *

FROM     
(
            SELECT      cid,
                        bdate = 
                        CASE
                            WHEN bdate > GETDATE() THEN NULL
                            ELSE
                                    bdate
                        END,
                        gen

            FROM    Datawarehouse.silver.[erp_cust_az12]
)
X



/************************************************************************************************************************************

    27b. Load Script
            
            Cleaning table [bronze].[erp_loc_a101]

*************************************************************************************************************************************/


SELECT      *

FROM        [Datawarehouse].[bronze].[erp_loc_a101]


/************************************************************************************************************************************

    Column cid:
    
    Check if cid from Table [bronze].[erp_loc_a101] matches with cst_key from table [bronze].[crm_cust_info]

            - We noticing that there is hypthen (-) in column [bronze].[erp_loc_a101]. This wont match with data from column 
            - We replacing (-) with nothing

*************************************************************************************************************************************/

--- just for display 

SELECT  distinct cid

FROM    Datawarehouse.[bronze].[erp_loc_a101]


SELECT distinct cst_key

FROM Datawarehouse.bronze.crm_cust_info


------------------------------------------------

SELECT  *

FROM    Datawarehouse.[bronze].[erp_loc_a101]

WHERE cid NOT IN
(
    SELECT cst_key

    FROM Datawarehouse.bronze.crm_cust_info
)


---- Addressing the issue of column cid by cleaning the data

SELECT      X.cid,
            X.cntry

FROM
(
    SELECT      cid = CAST(replace(cid, '-','') as nvarchar(50)),
                cntry

    FROM        Datawarehouse.[bronze].[erp_loc_a101]
)
X

WHERE X.cid NOT IN 
(
    SELECT      CAST(cst_id as nvarchar(50))

    FROM        Datawarehouse.[bronze].[crm_cust_info]
)



/************************************************************************************************************************************

    column cntry:

           - checking data consistency

*************************************************************************************************************************************/


SELECT distinct cntry,
        [My Transformation] = 
                CASE
                    WHEN cntry = '' OR cntry IS NULL THEN 'n/a'
                    WHEN TRIM(UPPER(cntry)) = 'DE' THEN 'Germany'                           ----- Data Consistency Addressed
                    WHEN cntry in ('united States', 'US', 'USA') THEN 'United States'       
                    Else                                                                        
                        cntry
                END

FROM   Datawarehouse.[bronze].[erp_loc_a101]

Order  BY cntry




---- Addressing the issue of Data Consistency

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


/***********************************************************************************************************************************

    27b. Final Code:
        Inserting the clean data from table [bronze].[erp_loc_a101] into table [Silver].[erp_loc_a101]

***********************************************************************************************************************************/

SELECT      *

FROM        Datawarehouse.bronze.erp_loc_a101


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



/***********************************************************************************************************************************

    27b. Doing checks on table Datawarehouse.[Silver].[erp_loc_a101]

************************************************************************************************************************************/

---- Checking column cid


SELECT  distinct cid

FROM    Datawarehouse.silver.[erp_loc_a101]


SELECT distinct cst_key

FROM Datawarehouse.silver.crm_cust_info


------------------------------------------------

SELECT  *

FROM    Datawarehouse.silver.[erp_loc_a101]

WHERE cid NOT IN
(
    SELECT cst_key

    FROM Datawarehouse.silver.crm_cust_info
)



------ Checking column cntry

SELECT distinct cntry,
        [My Transformation] = 
                CASE
                    WHEN cntry = '' OR cntry IS NULL THEN 'n/a'
                    WHEN TRIM(UPPER(cntry)) = 'DE' THEN 'Germany'                           ----- Data Consistency Addressed
                    WHEN cntry in ('united States', 'US', 'USA') THEN 'United States'       
                    Else                                                                        
                        cntry
                END

FROM   Datawarehouse.silver.[erp_loc_a101]

Order  BY cntry



/*************************************************************************************************************************************

    27c. Cleaning the table [bronze].[erp_px_cat_g1v2]

**************************************************************************************************************************************/

SELECT      *

FROM        Datawarehouse.[bronze].[erp_px_cat_g1v2]



/*********************************************************************************************************************************************************************

    27c. Column id:
        
            - Checking to see if the data in column id on table [bronze].[erp_px_cat_g1v2] matches data in column prd_key in table Datawarehouse.silver.[crm_prd_info]

**********************************************************************************************************************************************************************/


----------- Just for display 


SELECT      *

FROM        Datawarehouse.bronze.erp_px_cat_g1v2


SELECT     *

FROM        Datawarehouse.silver.[crm_prd_info]



--- Actual Check

SELECT      distinct  id

FROM          Datawarehouse.bronze.erp_px_cat_g1v2

WHERE id NOT IN 
(
    SELECT  cat_id

    FROM    Datawarehouse.silver.[crm_prd_info]   
   
)



/*********************************************************************************************************************************************************************

    27c. Column cat:
        
            - Checking data consistency
            - Check for Unwanted spaces

**********************************************************************************************************************************************************************/


---- Checking for Data Consistency

SELECT  distinct cat
      

FROM    Datawarehouse.bronze.erp_px_cat_g1v2


---- Check for Unwanted spaces
SELECT  distinct cat,
        [Space checking] = LEN(cat) - LEN(TRIM(cat))
      

FROM    Datawarehouse.bronze.erp_px_cat_g1v2


/*********************************************************************************************************************************************************************

    27c. Column subcat:
        
            - Checking data consistency
            - Check for Unwanted spaces

**********************************************************************************************************************************************************************/

---- Checking data consistency

SELECT          distinct subcat

FROM            Datawarehouse.bronze.erp_px_cat_g1v2



---- Check for Unwanted spaces

SELECT      *

FROM
(
    SELECT      distinct subcat,
                [Check for Unwanted spaces] = LEN(subcat) - LEN(TRIM(subcat))

    FROM        Datawarehouse.bronze.erp_px_cat_g1v2
)
X

WHERE           X.[Check for Unwanted spaces] > 0

Order BY        X.subcat




/*********************************************************************************************************************************************************************

    27c. Column maintenance:
        
            - Checking data consistency
   

**********************************************************************************************************************************************************************/

      
SELECT      distinct maintenance

FROM        Datawarehouse.bronze.erp_px_cat_g1v2



/**********************************************************************************************************************************************************************

    27c. table  Datawarehouse.silver.erp_px_cat_g1v2
            - Final and Clean code
            - No problem on this table

********************************************************************************************************************************************************************/

INSERT INTO Datawarehouse.silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)

SELECT      *

FROM        Datawarehouse.bronze.erp_px_cat_g1v2


/**********************************************************************************************************************************************************************

    27c. table  Datawarehouse.silver.erp_px_cat_g1v2
            - Testing to see if all records where inserted succesfully

********************************************************************************************************************************************************************/
