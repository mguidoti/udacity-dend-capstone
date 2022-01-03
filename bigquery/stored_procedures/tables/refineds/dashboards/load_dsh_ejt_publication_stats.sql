CREATE OR REPLACE PROCEDURE `poc-plazi-dashboards.ejt_v4.load_dsh_ejt_publication_stats`(VAR_PROJECT_ID STRING,
                                                                                         VAR_SANDBOX_ID STRING,
                                                                                         VAR_DATASET STRING,
                                                                                         VAR_LOAD_MODE STRING)

-- ################################################################################
-- ### Object: poc-plazi-dashboards.publications.load_dsh_ejt_publication_stats
-- ### Created at: 2022-01-02
-- ### Description: Stored Procedure to update FULL or DELTA the refined table
-- ###              poc-plazi-dashboards.publications.load_dsh_ejt_publication_stats
-- ### Author: Marcus Guidoti | Plazi's Data Engineer
-- ### Email: guidoti@plazi.org

-- ### Change Log:
-- ################################################################################

BEGIN

    -- This variable is shared between all called procedures, including this one
    DECLARE VAR_TABLE STRING DEFAULT 'ejt_publication_stats';

    -- These variables are needed to call the Stored Procedure `data-tools-prod-336318.config.get_params`()
    DECLARE AUX_DATASET STRING DEFAULT null;
    DECLARE AUX_TABLE STRING DEFAULT null;
    DECLARE DELTA_FIELD STRING DEFAULT null;
    DECLARE DELTA_INTERVAL_START INT64 DEFAULT null;
    DECLARE DELTA_INTERVAL_END INT64 DEFAULT null;
    DECLARE DELTA_START_DATE DATE DEFAULT null;
    DECLARE DELTA_END_DATE DATE DEFAULT null;

    -- These variables are needed to call the `data-tools-prod-336318.logs.write`() stored procedure
    -- and have hard-coded values
    DECLARE VAR_LOG_TABLE STRING DEFAULT 'data_updates';
    DECLARE VAR_SERVICE STRING DEFAULT 'BigQuery';
    DECLARE VAR_PROCESS STRING DEFAULT 'load_dsh_ejt_publication_stats';
    DECLARE VAR_ZONE STRING DEFAULT 'REFINED';
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

        -- Call `data-tools-prod-336318.config.get_params`()
        BEGIN

            CALL `data-tools-prod-336318.config.get_params`(VAR_PROJECT_ID, VAR_DATASET, VAR_TABLE, AUX_DATASET, AUX_TABLE, DELTA_FIELD, DELTA_INTERVAL_START, DELTA_INTERVAL_END, DELTA_START_DATE, DELTA_END_DATE);

            -- If the VAR_LOAD_MODE is set to "FULL", then redefine the DELTA_DATE variables to get the entire interval of data
            IF VAR_LOAD_MODE = 'FULL' THEN
                EXECUTE IMMEDIATE FORMAT("""
                    SELECT DATE(MIN(%s)),
                        DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)
                    FROM `%s.publications.treatment_bank_basic_stats`
                """,
                    DELTA_FIELD,
                    IFNULL(VAR_SANDBOX_ID, 'poc-plazi-trusted')
                )
                INTO DELTA_START_DATE, DELTA_END_DATE;
            END IF;

        EXCEPTION WHEN ERROR THEN

            -- Set the missing log variables in order to properly log the record
            SET VAR_STATUS = 'failed';
            SET VAR_RESPONSE = @@error.message;
            SET VAR_MESSAGE = 'Failed while calling the `data-tools-prod-336318.config.get_params`() or definying the date variables for the bulk process.';
            SET VAR_NUM_ROWS_INSERTED = 0;
            SET VAR_RAN_AT = CURRENT_DATETIME();

            -- Call the write procedure passing the arguments
            CALL `data-tools-prod-336318.logs.write`(VAR_LOG_TABLE, VAR_SERVICE, VAR_PROCESS, VAR_LOAD_MODE, VAR_ZONE, VAR_PROJECT_ID, VAR_DATASET, VAR_TABLE, VAR_START_PROCESS_DATE, VAR_STATUS, VAR_RESPONSE, VAR_MESSAGE, VAR_NUM_ROWS_INSERTED, VAR_LOGGED_FROM, VAR_RAN_AT);

            -- Raise an error with the error message
            RAISE USING MESSAGE = @@error.message;

        END;

        -- This block actually updates the data in the destination table
        BEGIN

            -- Create a temp table with the data to be updated
            EXECUTE IMMEDIATE FORMAT("""
                CREATE TEMP TABLE final_temp_table AS
                    SELECT article_uuid,
                           title,
                           year_published,
                           num_treatments,
                           num_treat_citations,
                           num_mat_citations,
                           num_pages,
                           num_figures,
                           num_tables,
                           num_references
                    FROM `%s.publications.treatment_bank_basic_stats`
                    WHERE upload_date BETWEEN '%s' AND '%s'
                    AND publication = 'European Journal of Taxonomy'
            """,
                IFNULL(VAR_SANDBOX_ID, 'poc-plazi-trusted'),
                CAST(DELTA_START_DATE AS STRING),
                CAST(DELTA_END_DATE AS STRING)
            );

            -- Load number of rows into proper variable
            EXECUTE IMMEDIATE"""
                SELECT COUNT(*)
                FROM final_temp_table
            """
            INTO VAR_NUM_ROWS_INSERTED;

            IF EXISTS (SELECT * FROM final_temp_table) THEN

                IF VAR_LOAD_MODE = 'FULL' THEN
                    EXECUTE IMMEDIATE FORMAT("""
                        TRUNCATE TABLE `%s.%s.%s`
                    """,
                        VAR_PROJECT_ID,
                        VAR_DATASET,
                        VAR_TABLE
                    );
                ELSE
                    -- Delete rows
                    EXECUTE IMMEDIATE FORMAT("""
                        DELETE FROM `%s.%s.%s`
                        WHERE article_uuid IN (SELECT DISTINCT article_uuid
                                               FROM final_temp_table)
                    """,
                        VAR_PROJECT_ID,
                        VAR_DATASET,
                        VAR_TABLE
                    );
                END IF;

                -- Insert updated rows
                EXECUTE IMMEDIATE FORMAT("""
                    INSERT INTO `%s.%s.%s` (article_uuid,
                                            title,
                                            year_published,
                                            num_treatments,
                                            num_treat_citations,
                                            num_mat_citations,
                                            num_pages,
                                            num_figures,
                                            num_tables,
                                            num_references)
                        SELECT *
                        FROM final_temp_table
                """,
                    VAR_PROJECT_ID,
                    VAR_DATASET,
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