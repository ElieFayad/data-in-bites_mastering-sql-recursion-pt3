/*
    This script contains all the data & solutions for the article's 1st use case: Product Assembly

    In this repo you can find bigger datasets w.r.t. the data used in the article, feel free to use them and experiment!
*/

-- Create the required tables

USE DATABASE RECURSIVE_CTE;
USE SCHEMA RECURSIVE_CTE_DATA;

CREATE OR REPLACE TABLE RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC1_PRODUCTS (
  PRODUCT_ID NUMBER,
  PRODUCT_NAME STRING,
  ASSEMBLY_TIME NUMBER,
  ASSEMBLY_COST NUMBER
);

CREATE OR REPLACE TABLE RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC1_PRODUCT_PARTS (
  PART_ID NUMBER,
  PART_NAME STRING,
  COMPONENT_ID NUMBER,
  QUANTITY NUMBER
);

CREATE OR REPLACE TABLE RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC1_COMPONENT_DETAILS (
  COMPONENT_ID NUMBER,
  COMPONENT_NAME STRING,
  COST NUMBER(38,2),
  DURATION NUMBER
);

-- Load data to the tables
-- if you want to experiment with other datasets, just change the referenced file

truncate table RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC1_PRODUCTS;
truncate table RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC1_PRODUCT_PARTS;
truncate table RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC1_COMPONENT_DETAILS;

COPY INTO RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC1_PRODUCTS
FROM @RECURSIVE_CTE_DATA.RECURSIVE_DATA_STAGE/uc1_products.json
FILE_FORMAT = (FORMAT_NAME = JSON_FF)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
;

SELECT * FROM RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC1_PRODUCTS;

COPY INTO RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC1_PRODUCT_PARTS
FROM @RECURSIVE_CTE_DATA.RECURSIVE_DATA_STAGE/uc1_product_parts.json
FILE_FORMAT = (FORMAT_NAME = JSON_FF)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
;

SELECT * FROM RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC1_PRODUCT_PARTS;

COPY INTO RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC1_COMPONENT_DETAILS
FROM @RECURSIVE_CTE_DATA.RECURSIVE_DATA_STAGE/uc1_component_details.json
FILE_FORMAT = (FORMAT_NAME = JSON_FF)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
;

SELECT * FROM RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC1_COMPONENT_DETAILS;


-- Solution Query
/******************************** STEP 1: Find all components needed to build the final product ********************************/
WITH
    ALL_COMPONENTS_PRODUCT AS (
        -- Anchor Member: Start with all the final products -> all products present in the UC1_PRODUCTS table
        
        SELECT 
            -- Final product ID
            PRODS.PRODUCT_ID AS TARGET_PRODUCT
            -- Component that the final product depends on
            , PRODS.PRODUCT_ID AS CURRENT_COMPONENT
            -- Cost of the current step
            , PRODS.ASSEMBLY_COST AS STEP_COST
            -- Duration of the current step
            , PRODS.ASSEMBLY_TIME AS STEP_DURATION
            -- Required quantities of the component
            , 1 AS REQUIRED_QUANTITY
            -- technical recursive columns
            , ARRAY_CONSTRUCT(PRODS.PRODUCT_ID) AS COMPONENTS_IN_PATH
            , 1 AS ITERATION_LEVEL
            /*THESE COLUMNS ARE JUST TO HELP VISUALIZE THE OUTPUT*/
            , PRODS.PRODUCT_NAME AS TARGET_PRODUCT_NAME
            , PRODS.PRODUCT_NAME AS CURRENT_COMPONENT_NAME
            , ARRAY_CONSTRUCT(PRODS.PRODUCT_NAME) AS COMPONENTS_NAME_IN_PATH
        FROM 
            RECURSIVE_CTE_DATA.UC1_PRODUCTS PRODS

        UNION ALL

        -- Recursive Member: Traverse the hierarchy from the top to the bottom to find all components the final product depends on
        
        SELECT 
            PREV_STEP.TARGET_PRODUCT
            , PROD_PART.COMPONENT_ID AS CURRENT_COMPONENT
            , COMP_DET.COST AS STEP_COST
            , COMP_DET.DURATION AS STEP_DURATION
            , PROD_PART.QUANTITY AS REQUIRED_QUANTITY
            , ARRAY_APPEND(PREV_STEP.COMPONENTS_IN_PATH, PROD_PART.COMPONENT_ID) AS COMPONENTS_IN_PATH
            , PREV_STEP.ITERATION_LEVEL + 1 AS ITERATION_LEVEL
            /*THESE COLUMNS ARE JUST TO HELP VISUALIZE THE OUTPUT*/
            , PREV_STEP.TARGET_PRODUCT_NAME
            , COMP_DET.COMPONENT_NAME AS CURRENT_COMPONENT_NAME
            , ARRAY_APPEND(PREV_STEP.COMPONENTS_NAME_IN_PATH, COMP_DET.COMPONENT_NAME) AS COMPONENTS_NAME_IN_PATH
        FROM 
            ALL_COMPONENTS_PRODUCT PREV_STEP
            INNER JOIN RECURSIVE_CTE_DATA.UC1_PRODUCT_PARTS PROD_PART -- get the component on which the parent component is dependent on
                ON PREV_STEP.CURRENT_COMPONENT = PROD_PART.PART_ID
            INNER JOIN RECURSIVE_CTE_DATA.UC1_COMPONENT_DETAILS COMP_DET -- get the details of the components
                ON COMP_DET.COMPONENT_ID = PROD_PART.COMPONENT_ID
        WHERE
            PREV_STEP.ITERATION_LEVEL <= 1000
            AND NOT ARRAY_CONTAINS(PROD_PART.COMPONENT_ID, PREV_STEP.COMPONENTS_IN_PATH)
    )
SELECT * 
FROM ALL_COMPONENTS_PRODUCT
ORDER BY TARGET_PRODUCT, ITERATION_LEVEL
;

/******************************** STEP 2: Aggregate the results! ********************************/
WITH
    ALL_COMPONENTS_PRODUCT AS (
        -- Anchor Member: Start with all the final products -> all products present in the UC1_PRODUCTS table
        
        SELECT 
            -- Final product ID
            PRODS.PRODUCT_ID AS TARGET_PRODUCT
            -- Component that the final product depends on
            , PRODS.PRODUCT_ID AS CURRENT_COMPONENT
            -- Cost of the current step
            , PRODS.ASSEMBLY_COST AS STEP_COST
            -- Duration of the current step
            , PRODS.ASSEMBLY_TIME AS STEP_DURATION
            -- Required quantities of the component
            , 1 AS REQUIRED_QUANTITY
            -- technical recursive columns
            , ARRAY_CONSTRUCT(PRODS.PRODUCT_ID) AS COMPONENTS_IN_PATH
            , 1 AS ITERATION_LEVEL
            /*THESE COLUMNS ARE JUST TO HELP VISUALIZE THE OUTPUT*/
            , PRODS.PRODUCT_NAME AS TARGET_PRODUCT_NAME
            , PRODS.PRODUCT_NAME AS CURRENT_COMPONENT_NAME
            , ARRAY_CONSTRUCT(PRODS.PRODUCT_NAME) AS COMPONENTS_NAME_IN_PATH
        FROM 
            RECURSIVE_CTE_DATA.UC1_PRODUCTS PRODS

        UNION ALL

        -- Recursive Member: Traverse the hierarchy from the top to the bottom to find all components the final product depends on
        
        SELECT 
            PREV_STEP.TARGET_PRODUCT
            , PROD_PART.COMPONENT_ID AS CURRENT_COMPONENT
            , COMP_DET.COST AS STEP_COST
            , COMP_DET.DURATION AS STEP_DURATION
            , PROD_PART.QUANTITY AS REQUIRED_QUANTITY
            , ARRAY_APPEND(PREV_STEP.COMPONENTS_IN_PATH, PROD_PART.COMPONENT_ID) AS COMPONENTS_IN_PATH
            , PREV_STEP.ITERATION_LEVEL + 1 AS ITERATION_LEVEL
            /*THESE COLUMNS ARE JUST TO HELP VISUALIZE THE OUTPUT*/
            , PREV_STEP.TARGET_PRODUCT_NAME
            , COMP_DET.COMPONENT_NAME AS CURRENT_COMPONENT_NAME
            , ARRAY_APPEND(PREV_STEP.COMPONENTS_NAME_IN_PATH, COMP_DET.COMPONENT_NAME) AS COMPONENTS_NAME_IN_PATH
        FROM 
            ALL_COMPONENTS_PRODUCT PREV_STEP
            INNER JOIN RECURSIVE_CTE_DATA.UC1_PRODUCT_PARTS PROD_PART -- get the component on which the parent component is dependent on
                ON PREV_STEP.CURRENT_COMPONENT = PROD_PART.PART_ID
            INNER JOIN RECURSIVE_CTE_DATA.UC1_COMPONENT_DETAILS COMP_DET -- get the details of the components
                ON COMP_DET.COMPONENT_ID = PROD_PART.COMPONENT_ID
        WHERE
            PREV_STEP.ITERATION_LEVEL <= 1000
            AND NOT ARRAY_CONTAINS(PROD_PART.COMPONENT_ID, PREV_STEP.COMPONENTS_IN_PATH)
    ), TOTALS_PER_COMPONENT as (
        -- Find the total quantity, cost, and duration for each component of the product
        SELECT 
            TARGET_PRODUCT
            , TARGET_PRODUCT_NAME
            , CURRENT_COMPONENT
            , CURRENT_COMPONENT_NAME
            , SUM(REQUIRED_QUANTITY * STEP_COST) AS REQUIRED_QUANTITY_COST
            , SUM(REQUIRED_QUANTITY * STEP_DURATION) AS REQUIRED_QUANTITY_DURATION
            , SUM(REQUIRED_QUANTITY) AS TOTAL_REQUIRED_QUANTITY
        FROM ALL_COMPONENTS_PRODUCT
        GROUP BY ALL
    )
-- Sum the costs and duration to get the final data
SELECT 
    TARGET_PRODUCT
    , TARGET_PRODUCT_NAME
    , SUM(REQUIRED_QUANTITY_COST) AS TOTAL_COST
    , SUM(REQUIRED_QUANTITY_DURATION) AS TOTAL_DURATION
    -- List all the components in the final product
    , OBJECT_DELETE(
        OBJECT_AGG(
            CURRENT_COMPONENT_NAME || '_' || CURRENT_COMPONENT, 
            TOTAL_REQUIRED_QUANTITY
        ), 
        TARGET_PRODUCT_NAME || '_' || TARGET_PRODUCT
    ) AS ALL_COMPONENTS
FROM TOTALS_PER_COMPONENT
GROUP BY ALL
ORDER BY TARGET_PRODUCT
;