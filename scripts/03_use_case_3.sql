/*
    This script contains all the data & solutions for the article's 3rd use case: Network Routing - Min/Max Hops

    In this repo you can find bigger datasets w.r.t. the data used in the article, feel free to use them and experiment!
*/

-- Create the required tables

USE DATABASE RECURSIVE_CTE;
USE SCHEMA RECURSIVE_CTE_DATA;

CREATE OR REPLACE TABLE RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC3_ROUTING_HUBS (
  HUB_ID NUMBER,
  HUB_LOCATION STRING
);

CREATE OR REPLACE TABLE RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC3_HUB_LINKS (
  SOURCE_HUB_ID NUMBER,
  TARGET_HUB_ID NUMBER,
  PATH_LENGTH_KM NUMBER
);

-- Load data to the tables
-- if you want to experiment with other datasets, just change the referenced file

truncate table RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC3_ROUTING_HUBS;
truncate table RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC3_HUB_LINKS;

COPY INTO RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC3_ROUTING_HUBS
FROM @RECURSIVE_CTE_DATA.RECURSIVE_DATA_STAGE/uc3_routing_hubs.json
FILE_FORMAT = (FORMAT_NAME = JSON_FF)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
;

SELECT * FROM RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC3_ROUTING_HUBS;

COPY INTO RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC3_HUB_LINKS
FROM @RECURSIVE_CTE_DATA.RECURSIVE_DATA_STAGE/uc3_hub_links.json
FILE_FORMAT = (FORMAT_NAME = JSON_FF)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
;

SELECT * FROM RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC3_HUB_LINKS;


-- Solution Query
/******************************** STEP 1: Obtain all possible paths from a source to all other reachable destinations ********************************/
WITH
    ALL_PATHS_TO_DESTINATION AS (

        -- Anchor Member: Initialize the source hub (Lebanon)
            
        SELECT
            -- SOURCE HUB
            HUB.HUB_ID AS SOURCE_HUB_ID
            , HUB.HUB_LOCATION AS SOURCE_HUB_LOCATION
            -- DESTINATION HUB
            , HUB.HUB_ID AS DESTINATION_HUB_ID
            , HUB.HUB_LOCATION AS DESTINATION_HUB_LOCATION
            -- Path length in KMs
            , 0 AS PATH_LENGTH_KM
            -- Number of hops to get to current destination
            , 0 AS NUMBER_OF_HOPS
            -- Path to get to current destination
            , HUB.HUB_ID::VARCHAR AS TRAVERSED_PATH
            -- technical recursive columns
            , 1 AS ITERATION_LEVEL
            , ARRAY_CONSTRUCT(HUB.HUB_ID) AS HUBS_IN_PATH
            /*THESE COLUMNS ARE JUST TO HELP VISUALIZE THE OUTPUT*/
            , HUB.HUB_LOCATION AS TRAVERSED_PATH_LOCATIONS
            , ARRAY_CONSTRUCT(HUB.HUB_LOCATION) AS HUB_LOCATIONS_IN_PATH
        FROM
            RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC3_ROUTING_HUBS HUB
        WHERE
            HUB.HUB_LOCATION = 'Lebanon'

        UNION ALL

        -- Recursive Member: Get next reachable hubs not yet visited

        SELECT
            SOURCE_HUB.SOURCE_HUB_ID
            , SOURCE_HUB.SOURCE_HUB_LOCATION
            , HUB_LINK.TARGET_HUB_ID AS DESTINATION_HUB_ID
            , TARGET_HUBS.HUB_LOCATION AS DESTINATION_HUB_LOCATION
            , SOURCE_HUB.PATH_LENGTH_KM + HUB_LINK.PATH_LENGTH_KM AS PATH_LENGTH_KM
            , SOURCE_HUB.NUMBER_OF_HOPS + 1 AS NUMBER_OF_HOPS
            , SOURCE_HUB.TRAVERSED_PATH || ' -> ' || HUB_LINK.TARGET_HUB_ID AS TRAVERSED_PATH
            , SOURCE_HUB.ITERATION_LEVEL + 1 AS ITERATION_LEVEL
            , ARRAY_APPEND(SOURCE_HUB.HUBS_IN_PATH, HUB_LINK.TARGET_HUB_ID) AS HUBS_IN_PATH
            /*THESE COLUMNS ARE JUST TO HELP VISUALIZE THE OUTPUT*/
            , SOURCE_HUB.TRAVERSED_PATH_LOCATIONS || ' -> ' || TARGET_HUBS.HUB_LOCATION AS TRAVERSED_PATH_LOCATIONS
            , ARRAY_APPEND(SOURCE_HUB.HUB_LOCATIONS_IN_PATH, TARGET_HUBS.HUB_LOCATION) AS HUB_LOCATIONS_IN_PATH
        FROM
            ALL_PATHS_TO_DESTINATION AS SOURCE_HUB
            INNER JOIN RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC3_HUB_LINKS HUB_LINK -- get the reachable hubs from current hub
                ON HUB_LINK.SOURCE_HUB_ID = SOURCE_HUB.DESTINATION_HUB_ID
            INNER JOIN RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC3_ROUTING_HUBS TARGET_HUBS -- get destination hub details
                ON TARGET_HUBS.HUB_ID = HUB_LINK.TARGET_HUB_ID
        WHERE
            /*
                In situations like this use case it is very important to define robust stop conditions since rows can be generated exponentially 

                Assuming 40 hubs, with approx. 4 outbound connections per hub, we can arrive to millions of records just on iteration 10
            */
            -- Reasonable hop limit
            SOURCE_HUB.NUMBER_OF_HOPS < 30 
            -- Prevent cycles in path
            AND NOT ARRAY_CONTAINS(HUB_LINK.TARGET_HUB_ID, SOURCE_HUB.HUBS_IN_PATH)
            -- Reasonable Distance threshold
            AND SOURCE_HUB.PATH_LENGTH_KM + HUB_LINK.PATH_LENGTH_KM < 40000  
    )
SELECT *
FROM ALL_PATHS_TO_DESTINATION
ORDER BY SOURCE_HUB_ID, DESTINATION_HUB_ID, NUMBER_OF_HOPS, PATH_LENGTH_KM
;

/******************************** STEP 2.1: Find for each source-destination the shortest path in terms of KM travelled ********************************/
WITH
    ALL_PATHS_TO_DESTINATION AS (

        -- Anchor Member: Initialize the source hub (Lebanon)
            
        SELECT
            -- SOURCE HUB
            HUB.HUB_ID AS SOURCE_HUB_ID
            , HUB.HUB_LOCATION AS SOURCE_HUB_LOCATION
            -- DESTINATION HUB
            , HUB.HUB_ID AS DESTINATION_HUB_ID
            , HUB.HUB_LOCATION AS DESTINATION_HUB_LOCATION
            -- Path length in KMs
            , 0 AS PATH_LENGTH_KM
            -- Number of hops to get to current destination
            , 0 AS NUMBER_OF_HOPS
            -- Path to get to current destination
            , HUB.HUB_ID::VARCHAR AS TRAVERSED_PATH
            -- technical recursive columns
            , 1 AS ITERATION_LEVEL
            , ARRAY_CONSTRUCT(HUB.HUB_ID) AS HUBS_IN_PATH
            /*THESE COLUMNS ARE JUST TO HELP VISUALIZE THE OUTPUT*/
            , HUB.HUB_LOCATION AS TRAVERSED_PATH_LOCATIONS
            , ARRAY_CONSTRUCT(HUB.HUB_LOCATION) AS HUB_LOCATIONS_IN_PATH
        FROM
            RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC3_ROUTING_HUBS HUB
        WHERE
            HUB.HUB_LOCATION = 'Lebanon'

        UNION ALL

        -- Recursive Member: Get next reachable hubs not yet visited

        SELECT
            SOURCE_HUB.SOURCE_HUB_ID
            , SOURCE_HUB.SOURCE_HUB_LOCATION
            , HUB_LINK.TARGET_HUB_ID AS DESTINATION_HUB_ID
            , TARGET_HUBS.HUB_LOCATION AS DESTINATION_HUB_LOCATION
            , SOURCE_HUB.PATH_LENGTH_KM + HUB_LINK.PATH_LENGTH_KM AS PATH_LENGTH_KM
            , SOURCE_HUB.NUMBER_OF_HOPS + 1 AS NUMBER_OF_HOPS
            , SOURCE_HUB.TRAVERSED_PATH || ' -> ' || HUB_LINK.TARGET_HUB_ID AS TRAVERSED_PATH
            , SOURCE_HUB.ITERATION_LEVEL + 1 AS ITERATION_LEVEL
            , ARRAY_APPEND(SOURCE_HUB.HUBS_IN_PATH, HUB_LINK.TARGET_HUB_ID) AS HUBS_IN_PATH
            /*THESE COLUMNS ARE JUST TO HELP VISUALIZE THE OUTPUT*/
            , SOURCE_HUB.TRAVERSED_PATH_LOCATIONS || ' -> ' || TARGET_HUBS.HUB_LOCATION AS TRAVERSED_PATH_LOCATIONS
            , ARRAY_APPEND(SOURCE_HUB.HUB_LOCATIONS_IN_PATH, TARGET_HUBS.HUB_LOCATION) AS HUB_LOCATIONS_IN_PATH
        FROM
            ALL_PATHS_TO_DESTINATION AS SOURCE_HUB
            INNER JOIN RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC3_HUB_LINKS HUB_LINK -- get the reachable hubs from current hub
                ON HUB_LINK.SOURCE_HUB_ID = SOURCE_HUB.DESTINATION_HUB_ID
            INNER JOIN RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC3_ROUTING_HUBS TARGET_HUBS -- get destination hub details
                ON TARGET_HUBS.HUB_ID = HUB_LINK.TARGET_HUB_ID
        WHERE
            /*
                In situations like this use case it is very important to define robust stop conditions since rows can be generated exponentially 

                Assuming 40 hubs, with approx. 4 outbound connections per hub, we can arrive to millions of records just on iteration 10
            */
            -- Reasonable hop limit
            SOURCE_HUB.NUMBER_OF_HOPS < 30 
            -- Prevent cycles in path
            AND NOT ARRAY_CONTAINS(HUB_LINK.TARGET_HUB_ID, SOURCE_HUB.HUBS_IN_PATH)
            -- Reasonable Distance threshold
            AND SOURCE_HUB.PATH_LENGTH_KM + HUB_LINK.PATH_LENGTH_KM < 40000  
    )
SELECT 
     SOURCE_HUB_ID
    , SOURCE_HUB_LOCATION
    , DESTINATION_HUB_ID
    , DESTINATION_HUB_LOCATION
    , PATH_LENGTH_KM
    , NUMBER_OF_HOPS
    , TRAVERSED_PATH
    , ITERATION_LEVEL
    , HUBS_IN_PATH
    , TRAVERSED_PATH_LOCATIONS
    , HUB_LOCATIONS_IN_PATH
FROM ALL_PATHS_TO_DESTINATION
QUALIFY ROW_NUMBER() OVER (
                -- For every calculated source-destination path
                PARTITION BY SOURCE_HUB_ID, DESTINATION_HUB_ID
                --Take the shortest path in terms of KMs
                ORDER BY PATH_LENGTH_KM ASC
            ) = 1
ORDER BY SOURCE_HUB_ID, DESTINATION_HUB_ID, NUMBER_OF_HOPS, PATH_LENGTH_KM
;

/******************************** STEP 2.2: Find for each source-destination the shortest path in terms of hops ********************************/
WITH
    ALL_PATHS_TO_DESTINATION AS (

        -- Anchor Member: Initialize the source hub (Lebanon)
            
        SELECT
            -- SOURCE HUB
            HUB.HUB_ID AS SOURCE_HUB_ID
            , HUB.HUB_LOCATION AS SOURCE_HUB_LOCATION
            -- DESTINATION HUB
            , HUB.HUB_ID AS DESTINATION_HUB_ID
            , HUB.HUB_LOCATION AS DESTINATION_HUB_LOCATION
            -- Path length in KMs
            , 0 AS PATH_LENGTH_KM
            -- Number of hops to get to current destination
            , 0 AS NUMBER_OF_HOPS
            -- Path to get to current destination
            , HUB.HUB_ID::VARCHAR AS TRAVERSED_PATH
            -- technical recursive columns
            , 1 AS ITERATION_LEVEL
            , ARRAY_CONSTRUCT(HUB.HUB_ID) AS HUBS_IN_PATH
            /*THESE COLUMNS ARE JUST TO HELP VISUALIZE THE OUTPUT*/
            , HUB.HUB_LOCATION AS TRAVERSED_PATH_LOCATIONS
            , ARRAY_CONSTRUCT(HUB.HUB_LOCATION) AS HUB_LOCATIONS_IN_PATH
        FROM
            RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC3_ROUTING_HUBS HUB
        WHERE
            HUB.HUB_LOCATION = 'Lebanon'

        UNION ALL

        -- Recursive Member: Get next reachable hubs not yet visited

        SELECT
            SOURCE_HUB.SOURCE_HUB_ID
            , SOURCE_HUB.SOURCE_HUB_LOCATION
            , HUB_LINK.TARGET_HUB_ID AS DESTINATION_HUB_ID
            , TARGET_HUBS.HUB_LOCATION AS DESTINATION_HUB_LOCATION
            , SOURCE_HUB.PATH_LENGTH_KM + HUB_LINK.PATH_LENGTH_KM AS PATH_LENGTH_KM
            , SOURCE_HUB.NUMBER_OF_HOPS + 1 AS NUMBER_OF_HOPS
            , SOURCE_HUB.TRAVERSED_PATH || ' -> ' || HUB_LINK.TARGET_HUB_ID AS TRAVERSED_PATH
            , SOURCE_HUB.ITERATION_LEVEL + 1 AS ITERATION_LEVEL
            , ARRAY_APPEND(SOURCE_HUB.HUBS_IN_PATH, HUB_LINK.TARGET_HUB_ID) AS HUBS_IN_PATH
            /*THESE COLUMNS ARE JUST TO HELP VISUALIZE THE OUTPUT*/
            , SOURCE_HUB.TRAVERSED_PATH_LOCATIONS || ' -> ' || TARGET_HUBS.HUB_LOCATION AS TRAVERSED_PATH_LOCATIONS
            , ARRAY_APPEND(SOURCE_HUB.HUB_LOCATIONS_IN_PATH, TARGET_HUBS.HUB_LOCATION) AS HUB_LOCATIONS_IN_PATH
        FROM
            ALL_PATHS_TO_DESTINATION AS SOURCE_HUB
            INNER JOIN RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC3_HUB_LINKS HUB_LINK -- get the reachable hubs from current hub
                ON HUB_LINK.SOURCE_HUB_ID = SOURCE_HUB.DESTINATION_HUB_ID
            INNER JOIN RECURSIVE_CTE.RECURSIVE_CTE_DATA.UC3_ROUTING_HUBS TARGET_HUBS -- get destination hub details
                ON TARGET_HUBS.HUB_ID = HUB_LINK.TARGET_HUB_ID
        WHERE
            /*
                In situations like this use case it is very important to define robust stop conditions since rows can be generated exponentially 

                Assuming 40 hubs, with approx. 4 outbound connections per hub, we can arrive to millions of records just on iteration 10
            */
            -- Reasonable hop limit
            SOURCE_HUB.NUMBER_OF_HOPS < 30 
            -- Prevent cycles in path
            AND NOT ARRAY_CONTAINS(HUB_LINK.TARGET_HUB_ID, SOURCE_HUB.HUBS_IN_PATH)
            -- Reasonable Distance threshold
            AND SOURCE_HUB.PATH_LENGTH_KM + HUB_LINK.PATH_LENGTH_KM < 40000  
    )
SELECT 
     SOURCE_HUB_ID
    , SOURCE_HUB_LOCATION
    , DESTINATION_HUB_ID
    , DESTINATION_HUB_LOCATION
    , PATH_LENGTH_KM
    , NUMBER_OF_HOPS
    , TRAVERSED_PATH
    , ITERATION_LEVEL
    , HUBS_IN_PATH
    , TRAVERSED_PATH_LOCATIONS
    , HUB_LOCATIONS_IN_PATH
FROM ALL_PATHS_TO_DESTINATION
QUALIFY ROW_NUMBER() OVER (
                -- For every calculated source-destination path
                PARTITION BY SOURCE_HUB_ID, DESTINATION_HUB_ID
                --Take the shortest path in terms of hops
                ORDER BY NUMBER_OF_HOPS ASC
            ) = 1
ORDER BY SOURCE_HUB_ID, DESTINATION_HUB_ID, NUMBER_OF_HOPS, PATH_LENGTH_KM
;