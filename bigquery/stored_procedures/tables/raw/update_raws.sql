CREATE OR REPLACE PROCEDURE `poc-plazi-raw.apis.update_raws`(VAR_PROJECT_ID STRING,
                                                             VAR_SANDBOX_ID STRING,
                                                             VAR_DATASET STRING,
                                                             VAR_TABLE STRING,
                                                             VAR_PRIMARY_KEY STRING,
                                                             VAR_LOAD_MODE STRING)

-- ################################################################################
-- ### Object: poc-plazi-raw.apis.update_raw_tb_diostats_publications_stats
-- ### Created at: 2022-01-01
-- ### Description: Stored Procedure to update the raw tables from Treatment Bank
-- ### Author: Marcus Guidoti | Plazi's Data Engineer
-- ### Email: guidoti@plazi.org

-- ### Change Log:
-- ################################################################################

BEGIN

    -- These variables are needed to call the `data-tools-prod-336318.logs.write`() stored procedure
    -- and have hard-coded values
    DECLARE VAR_LOG_TABLE STRING DEFAULT 'ingestion';
    DECLARE VAR_SERVICE STRING DEFAULT 'BigQuery';
    DECLARE VAR_PROCESS STRING DEFAULT 'update_raws';
    DECLARE VAR_ZONE STRING DEFAULT 'RAW';
    DECLARE VAR_LOGGED_FROM STRING DEFAULT 'BigQuery';
    DECLARE VAR_START_PROCESS_DATE DATE DEFAULT DATE(CURRENT_DATE);

    -- These variables are needed to call the `data-tools-prod-336318.logs.write`() stored procedure
    -- and are filled according to the circunstances
    DECLARE VAR_STATUS STRING DEFAULT null;
    DECLARE VAR_RESPONSE STRING DEFAULT null;
    DECLARE VAR_MESSAGE STRING DEFAULT null;
    DECLARE VAR_NUM_ROWS_INSERTED INT64 DEFAULT null;
    DECLARE VAR_RAN_AT DATETIME DEFAULT null;

    BEGIN

        -- This block actually updates the data in the destination table
        BEGIN

            -- Load number of rows into proper variable
            EXECUTE IMMEDIATE FORMAT("""
                SELECT COUNT(*)
                FROM (SELECT DISTINCT *
                      FROM `%s.aux.%s_aux`)
            """,
                VAR_PROJECT_ID,
                VAR_TABLE
            )
            INTO VAR_NUM_ROWS_INSERTED;

            IF VAR_NUM_ROWS_INSERTED > 0 THEN

                -- Delete rows
                EXECUTE IMMEDIATE FORMAT("""
                    DELETE FROM `%s.%s.%s`
                    WHERE %s IN (SELECT DISTINCT %s
                                 FROM `%s.aux.%s_aux`)
                """,
                    VAR_PROJECT_ID,
                    VAR_DATASET,
                    VAR_TABLE,
                    VAR_PRIMARY_KEY,
                    VAR_PRIMARY_KEY,
                    IFNULL(VAR_SANDBOX_ID, 'poc-plazi-raw'),
                    VAR_TABLE
                );

                -- Insert updated rows
                EXECUTE IMMEDIATE FORMAT("""
                    INSERT INTO `%s.%s.%s`
                        SELECT DISTINCT *
                        FROM `%s.aux.%s_aux`
                """,
                    VAR_PROJECT_ID,
                    VAR_DATASET,
                    VAR_TABLE,
                    IFNULL(VAR_SANDBOX_ID, 'poc-plazi-raw'),
                    VAR_TABLE
                );

                -- Set the missing log variables in order to properly log the record
                SET VAR_STATUS = 'success';
                SET VAR_RESPONSE = '';
                SET VAR_MESSAGE = 'Succesfully loaded the data into the destination table.';
                --VAR_NUM_ROWS_INSERTED was already defined
                SET VAR_RAN_AT = CURRENT_DATETIME();

                -- Call the write procedure passing the arguments
                CALL `data-tools-prod-336318.logs.write`(VAR_LOG_TABLE, VAR_SERVICE, VAR_PROCESS, VAR_LOAD_MODE, VAR_ZONE, VAR_PROJECT_ID, VAR_DATASET, VAR_TABLE, VAR_START_PROCESS_DATE, VAR_STATUS, VAR_RESPONSE, VAR_MESSAGE, VAR_NUM_ROWS_INSERTED, VAR_LOGGED_FROM, VAR_RAN_AT);

            END IF;

        EXCEPTION WHEN ERROR THEN

            -- Set the missing log variables in order to properly log the record
            SET VAR_STATUS = 'failed';
            SET VAR_RESPONSE = @@error.message;
            SET VAR_MESSAGE = 'Failed while preparing or loading the data into the destination table.';
            SET VAR_NUM_ROWS_INSERTED = 0;
            SET VAR_RAN_AT = CURRENT_DATETIME();

            -- Call the write procedure passing the arguments
            CALL `data-tools-prod-336318.logs.write`(VAR_LOG_TABLE, VAR_SERVICE, VAR_PROCESS, VAR_LOAD_MODE, VAR_ZONE, VAR_PROJECT_ID, VAR_DATASET, VAR_TABLE, VAR_START_PROCESS_DATE, VAR_STATUS, VAR_RESPONSE, VAR_MESSAGE, VAR_NUM_ROWS_INSERTED, VAR_LOGGED_FROM, VAR_RAN_AT);

            -- Raise an error with the error message
            RAISE USING MESSAGE = @@error.message;

        END;

    EXCEPTION WHEN ERROR THEN

        -- Set the missing log variables in order to properly log the record
        SET VAR_STATUS = 'failed';
        SET VAR_RESPONSE = @@error.message;
        SET VAR_MESSAGE = 'Failed somewhere in the process of the procedure. Possibly bad input data type of parameters.';
        SET VAR_NUM_ROWS_INSERTED = 0;
        SET VAR_RAN_AT = CURRENT_DATETIME();

        -- Call the write procedure passing the arguments
        CALL `data-tools-prod-336318.logs.write`(VAR_LOG_TABLE, VAR_SERVICE, VAR_PROCESS, VAR_LOAD_MODE, VAR_ZONE, VAR_PROJECT_ID, VAR_DATASET, VAR_TABLE, VAR_START_PROCESS_DATE, VAR_STATUS, VAR_RESPONSE, VAR_MESSAGE, VAR_NUM_ROWS_INSERTED, VAR_LOGGED_FROM, VAR_RAN_AT);

        -- Raise an error with the error message
        RAISE USING MESSAGE = @@error.message;
    END;
END;