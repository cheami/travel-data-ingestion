CREATE OR REPLACE PROCEDURE TRAVEL_DATA.GOLD.SP_FULL_TRAVEL_COST()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
    -- 1. Create (or Replace) the Gold table
    CREATE OR REPLACE TABLE TRAVEL_DATA.GOLD.FULL_TRAVEL_COST AS

    WITH 
    -- LOGIC 1: Handle "Re-uploaded Files" for Spending
    -- We want ALL rows from the latest load of a specific file.
    -- QUALIFY LOAD_ID = MAX(...) ensures we keep the whole batch of rows from the newest load.
    SPENDING_FILE_FILTER AS (
        SELECT * FROM TRAVEL_DATA.SILVER.ALL_SPENDING
        QUALIFY LOAD_ID = MAX(LOAD_ID) OVER (PARTITION BY _SOURCE_FILE)
    ),

    -- LOGIC 2: Handle "Updated Daily Logs" for Manual Logs
    -- We want ONE row per Date. If multiple loads exist for ''2025-02-25'', pick the latest.
    LOGS_DATE_FILTER AS (
        SELECT * FROM TRAVEL_DATA.SILVER.MANUAL_LOGS
        QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE ORDER BY LOAD_ID DESC) = 1
    ),

    -- 3. Pivot Spending & Aggregate Comments
    SPENDING_PIVOT AS (
        SELECT 
            TRY_TO_DATE(DATE) AS JOIN_DATE,
            
            -- Sum costs by category (Pivot)
            SUM(CASE WHEN UPPER(TRIM(TYPE)) = ''HOTEL'' THEN AMOUNT ELSE 0 END) AS HOTEL_COST,
            SUM(CASE WHEN UPPER(TRIM(TYPE)) = ''FOOD'' THEN AMOUNT ELSE 0 END) AS FOOD_COST,
            SUM(CASE WHEN UPPER(TRIM(TYPE)) = ''ACTIVITY'' THEN AMOUNT ELSE 0 END) AS ACTIVITY_COST,
            SUM(CASE WHEN UPPER(TRIM(TYPE)) = ''TRAVEL'' THEN AMOUNT ELSE 0 END) AS TRAVEL_COST,
            SUM(CASE WHEN UPPER(TRIM(TYPE)) = ''MISC'' THEN AMOUNT ELSE 0 END) AS MISC_COST,
            
            -- Combine comments (e.g., "Uber; Train ticket")
            LISTAGG(COMMENTS, ''; '') WITHIN GROUP (ORDER BY COMMENTS) AS COST_COMMENT
        FROM SPENDING_FILE_FILTER
        WHERE TRY_TO_DATE(DATE) IS NOT NULL
        GROUP BY 1
    ),

    -- 4. Join Data Sets
    JOINED_DATA AS (
        SELECT 
            L.DAY,
            L.DATE, 
            TRY_TO_DATE(L.DATE) AS ORDER_DATE, -- FIXED: Moved alias inside the function
            L.CITY,
            L.COUNTY,
            L.DESCRIPTION,
            
            -- Log Details
            L.COMMENTS AS LOG_COMMENT,
            L.FOOD AS FOOD_DESC,
            L.TRAVEL AS TRAVEL_DESC,
            L.HOTEL AS HOTEL_DESC,
            
            -- Spending Columns (Coalesced to 0)
            COALESCE(S.HOTEL_COST, 0) AS HOTEL,
            COALESCE(S.FOOD_COST, 0) AS FOOD,
            COALESCE(S.ACTIVITY_COST, 0) AS ACTIVITY,
            COALESCE(S.TRAVEL_COST, 0) AS TRAVEL,
            COALESCE(S.MISC_COST, 0) AS MISC,
            S.COST_COMMENT,
            
            -- Daily Total Calculation
            (COALESCE(S.HOTEL_COST, 0) + COALESCE(S.FOOD_COST, 0) + 
             COALESCE(S.ACTIVITY_COST, 0) + COALESCE(S.TRAVEL_COST, 0) + 
             COALESCE(S.MISC_COST, 0)) AS TOTAL
        FROM LOGS_DATE_FILTER L
        LEFT JOIN SPENDING_PIVOT S ON TRY_TO_DATE(L.DATE) = S.JOIN_DATE
    )

    -- 5. Final Select with Window Functions
    SELECT
        DAY,
        DATE,
        CITY,
        COUNTY,
        DESCRIPTION, 
        
        -- Financials
        HOTEL,
        FOOD,
        ACTIVITY,
        TRAVEL,
        MISC,
        TOTAL,
        
        -- Running Total
        SUM(TOTAL) OVER (ORDER BY ORDER_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RUNNING_TOTAL,
        
        -- Daily Average
        CASE 
            WHEN DAY > 0 THEN 
                SUM(TOTAL) OVER (ORDER BY ORDER_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / DAY
            ELSE 0 
        END AS DAILY_AVG,
        
        -- Context
        COST_COMMENT, 
        LOG_COMMENT AS COMMENTS,
        FOOD_DESC,
        TRAVEL_DESC,
        HOTEL_DESC
    FROM JOINED_DATA
    ORDER BY ORDER_DATE ASC;

    RETURN ''SUCCESS: Gold table FULL_TRAVEL_COST created.'';
END;
';