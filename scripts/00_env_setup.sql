CREATE DATABASE RECURSIVE_CTE;
CREATE SCHEMA RECURSIVE_CTE_DATA;
DROP SCHEMA PUBLIC;

-- Create the stage and the file format
create file format RECURSIVE_CTE_DATA.JSON_FF
    type = JSON
    TRIM_SPACE = TRUE
    STRIP_OUTER_ARRAY = TRUE
    comment = 'File Format to help us parse the JSON files in the stage'
;

CREATE STAGE RECURSIVE_CTE_DATA.RECURSIVE_DATA_STAGE 
	DIRECTORY = ( ENABLE = true ) 
    FILE_FORMAT = (FORMAT_NAME = 'JSON_FF')
	COMMENT = 'Stage that will contain the JSON data files for the Recusive Data Examples'
;

-- Upload the JSON files in the directory /datasets/ to the snowflake stage 'RECURSIVE_DATA_STAGE'
-- You can either upload them manually through Snowflake's UI: https://docs.snowflake.com/en/user-guide/data-load-local-file-system-stage-ui#uploading-files-onto-a-stage
-- or following the steps in this link: https://docs.snowflake.com/en/user-guide/data-load-local-file-system-stage

-- list all the files in the stage to make sure that the upload was successful
ls @RECURSIVE_CTE.RECURSIVE_CTE_DATA.RECURSIVE_DATA_STAGE;

-- query the data in the files
SELECT $1 as Raw_Data
FROM @RECURSIVE_CTE_DATA.RECURSIVE_DATA_STAGE/uc1_product_parts.json
;
