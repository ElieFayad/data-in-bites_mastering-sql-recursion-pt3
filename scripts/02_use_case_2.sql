/*
    This script contains all the data & solutions for the article's 2nd use case: Budget Allocated vs. Actual Spend Across Cost Centers

    In this repo you can find bigger datasets w.r.t. the data used in the article, feel free to use them and experiment!
*/
-- Create the required tables

USE DATABASE RECURSIVE_CTE;
USE SCHEMA RECURSIVE_CTE_DATA;

CREATE OR REPLACE TABLE RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC2_COST_CENTERS (
  COST_CENTER_ID NUMBER,
  PARENT_CC_ID NUMBER,
  COST_CENTER_NAME STRING
);

CREATE OR REPLACE TABLE RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC2_COST_CENTERS_BUDGETS (
  COST_CENTER_ID NUMBER,
  REFERENCE_YEAR NUMBER,
  ALLOCATED_BUDGET NUMBER,
  TOTAL_SPENT NUMBER
);

-- Load data to the tables
-- if you want to experiment with other datasets, just change the referenced file

truncate table RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC2_COST_CENTERS;
truncate table RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC2_COST_CENTERS_BUDGETS;

COPY INTO RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC2_COST_CENTERS
FROM @RECURSIVE_CTE_DATA.RECURSIVE_DATA_STAGE/uc2_cost_centers.json
FILE_FORMAT = (FORMAT_NAME = JSON_FF)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
;

SELECT * FROM RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC2_COST_CENTERS;

COPY INTO RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC2_COST_CENTERS_BUDGETS
FROM @RECURSIVE_CTE_DATA.RECURSIVE_DATA_STAGE/uc2_cost_centers_budgets.json
FILE_FORMAT = (FORMAT_NAME = JSON_FF)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
;

SELECT * FROM RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC2_COST_CENTERS_BUDGETS;


-- Solution Query
/******************************** STEP 1: Get all children cost centers ********************************/
WITH
    COST_CENTER_ROLL_UP AS (

        -- Anchor Member: Get all cost centers directly under Corporate HQ
            
        SELECT
            -- Parent cost center
            CC.COST_CENTER_ID AS ROOT_COST_CENTER
            , CC.COST_CENTER_NAME AS ROOT_COST_CENTER_NAME
            -- The reference year
            , CC_B.REFERENCE_YEAR
            -- The child cost center
            , CC.COST_CENTER_ID AS CURRENT_CC
            -- Cost center budget
            , CC_B.ALLOCATED_BUDGET
            -- Cost center total yearly spent
            , CC_B.TOTAL_SPENT
            -- technical recursive columns
            , 1 AS ITERATION_LEVEL
            , ARRAY_CONSTRUCT(CC.COST_CENTER_ID) AS CCS_IN_PATH
            /*THESE COLUMNS ARE JUST TO HELP VISUALIZE THE OUTPUT*/
            , CC.COST_CENTER_NAME AS CURRENT_CC_NAME
            , ARRAY_CONSTRUCT(CC.COST_CENTER_NAME) AS CCS_IN_PATH_NAME
        FROM
            RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC2_COST_CENTERS CC
            INNER JOIN RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC2_COST_CENTERS_BUDGETS CC_B
                ON CC_B.COST_CENTER_ID = CC.COST_CENTER_ID
        WHERE
            CC.PARENT_CC_ID = 1

        UNION ALL

        -- Recursive Member: Traverse the hierarchy to find all descendant cost centers

        SELECT
            PARENT_CC.ROOT_COST_CENTER
            , PARENT_CC.ROOT_COST_CENTER_NAME
            , PARENT_CC.REFERENCE_YEAR
            , CC.COST_CENTER_ID AS CURRENT_CC
            , CC_B.ALLOCATED_BUDGET
            , CC_B.TOTAL_SPENT
            , PARENT_CC.ITERATION_LEVEL + 1 AS ITERATION_LEVEL
            , ARRAY_APPEND(PARENT_CC.CCS_IN_PATH, CC.COST_CENTER_ID) AS CCS_IN_PATH
            /*THESE COLUMNS ARE JUST TO HELP VISUALIZE THE OUTPUT*/
            , CC.COST_CENTER_NAME AS CURRENT_CC_NAME
            , ARRAY_APPEND(PARENT_CC.CCS_IN_PATH_NAME, CC.COST_CENTER_NAME) AS CCS_IN_PATH_NAME
        FROM
            COST_CENTER_ROLL_UP PARENT_CC
            INNER JOIN RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC2_COST_CENTERS CC -- get the children of the parent cost centers of the previous step
                ON CC.PARENT_CC_ID = PARENT_CC.CURRENT_CC
            INNER JOIN RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC2_COST_CENTERS_BUDGETS CC_B -- get the related data of the child cost center
                ON CC_B.COST_CENTER_ID = CC.COST_CENTER_ID
                    AND CC_B.REFERENCE_YEAR = PARENT_CC.REFERENCE_YEAR
        WHERE
            PARENT_CC.ITERATION_LEVEL < 1000
            AND NOT ARRAY_CONTAINS(CC.COST_CENTER_ID, PARENT_CC.CCS_IN_PATH)
    )
SELECT *
FROM COST_CENTER_ROLL_UP
ORDER BY ROOT_COST_CENTER, REFERENCE_YEAR, CURRENT_CC
;

/******************************** STEP 2: Find the total aggregate budget & amount spent ********************************/
WITH
    COST_CENTER_ROLL_UP AS (

        -- Anchor Member: Get all cost centers directly under Corporate HQ
            
        SELECT
            -- Parent cost center
            CC.COST_CENTER_ID AS ROOT_COST_CENTER
            , CC.COST_CENTER_NAME AS ROOT_COST_CENTER_NAME
            -- The reference year
            , CC_B.REFERENCE_YEAR
            -- The child cost center
            , CC.COST_CENTER_ID AS CURRENT_CC
            -- Cost center budget
            , CC_B.ALLOCATED_BUDGET
            -- Cost center total yearly spent
            , CC_B.TOTAL_SPENT
            -- technical recursive columns
            , 1 AS ITERATION_LEVEL
            , ARRAY_CONSTRUCT(CC.COST_CENTER_ID) AS CCS_IN_PATH
            /*THESE COLUMNS ARE JUST TO HELP VISUALIZE THE OUTPUT*/
            , CC.COST_CENTER_NAME AS CURRENT_CC_NAME
            , ARRAY_CONSTRUCT(CC.COST_CENTER_NAME) AS CCS_IN_PATH_NAME
        FROM
            RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC2_COST_CENTERS CC
            INNER JOIN RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC2_COST_CENTERS_BUDGETS CC_B
                ON CC_B.COST_CENTER_ID = CC.COST_CENTER_ID
        WHERE
            CC.PARENT_CC_ID = 1

        UNION ALL

        -- Recursive Member: Traverse the hierarchy to find all descendant cost centers

        SELECT
            PARENT_CC.ROOT_COST_CENTER
            , PARENT_CC.ROOT_COST_CENTER_NAME
            , PARENT_CC.REFERENCE_YEAR
            , CC.COST_CENTER_ID AS CURRENT_CC
            , CC_B.ALLOCATED_BUDGET
            , CC_B.TOTAL_SPENT
            , PARENT_CC.ITERATION_LEVEL + 1 AS ITERATION_LEVEL
            , ARRAY_APPEND(PARENT_CC.CCS_IN_PATH, CC.COST_CENTER_ID) AS CCS_IN_PATH
            /*THESE COLUMNS ARE JUST TO HELP VISUALIZE THE OUTPUT*/
            , CC.COST_CENTER_NAME AS CURRENT_CC_NAME
            , ARRAY_APPEND(PARENT_CC.CCS_IN_PATH_NAME, CC.COST_CENTER_NAME) AS CCS_IN_PATH_NAME
        FROM
            COST_CENTER_ROLL_UP PARENT_CC
            INNER JOIN RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC2_COST_CENTERS CC -- get the children of the parent cost centers of the previous step
                ON CC.PARENT_CC_ID = PARENT_CC.CURRENT_CC
            INNER JOIN RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC2_COST_CENTERS_BUDGETS CC_B -- get the related data of the child cost center
                ON CC_B.COST_CENTER_ID = CC.COST_CENTER_ID
                    AND CC_B.REFERENCE_YEAR = PARENT_CC.REFERENCE_YEAR
        WHERE
            PARENT_CC.ITERATION_LEVEL < 1000
            AND NOT ARRAY_CONTAINS(CC.COST_CENTER_ID, PARENT_CC.CCS_IN_PATH)
    )
SELECT 
    ROOT_COST_CENTER AS COST_CENTER
    , ROOT_COST_CENTER_NAME AS COST_CENTER_NAME
    , REFERENCE_YEAR
    -- get the total allocated budget
    , SUM(ALLOCATED_BUDGET) AS TOTAL_BUDGET_CC
    -- get the total effective spent
    , SUM(TOTAL_SPENT) AS TOTAL_SPENT_CC
    -- calculate the difference between budget and actual spent
    , TOTAL_BUDGET_CC - TOTAL_SPENT_CC AS BUDGET_VARIANCE
    , ROUND((BUDGET_VARIANCE / TOTAL_BUDGET_CC) * 100, 2) AS BUDGET_VARIANCE_PCT
    -- classify the cost center
    , CASE
        WHEN BUDGET_VARIANCE < 0 THEN 'Over Budget'
        WHEN BUDGET_VARIANCE = 0 THEN 'Within Budget'
        WHEN BUDGET_VARIANCE > 0 THEN 'Under Budget'
      END BUDGET_STATUS
FROM COST_CENTER_ROLL_UP
GROUP BY ALL
ORDER BY COST_CENTER, REFERENCE_YEAR
;