CREATE OR REPLACE PROCEDURE `poc-plazi-trusted.publications.load_trd_treatment_bank_basic_stats`(VAR_PROJECT_ID STRING,
                                                                                                             VAR_SANDBOX_ID STRING,
                                                                                                             VAR_DATASET STRING,
                                                                                                             VAR_LOAD_MODE STRING)

-- ################################################################################
-- ### Object: data-tools-prod-336318.config.get_params
-- ### Created at: 2021-12-29
-- ### Description: Stored Procedure to update FULL or DELTA the trusted table
-- ###              `poc-plazi-trusted.publications.treatment_bank_basic_stats`
-- ### Author: Marcus Guidoti | Plazi's Data Engineer
-- ### Email: guidoti@plazi.org

-- ### Change Log:
-- ################################################################################

BEGIN

    -- This variable is shared between all called procedures, including this one
    DECLARE VAR_TABLE STRING DEFAULT 'treatment_bank_basic_stats';

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
    DECLARE VAR_PROCESS STRING DEFAULT 'load_trd_treatment_bank_basic_stats';
    DECLARE VAR_ZONE STRING DEFAULT 'TRUSTED';
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
                    FROM `%s.apis.tb_diostats_publications_stats`
                """,
                    DELTA_FIELD,
                    IFNULL(VAR_SANDBOX_ID, 'poc-plazi-raw')
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
                    SELECT DISTINCT DocArticleUuid AS article_uuid,
                                    DocName AS document_name,
                                    CASE
                                        WHEN DocDoi = '' THEN null
                                        ELSE DocDoi
                                    END AS doi,
                                    DocUploadUser AS upload_user,
                                    DATE(DocUploadDate) AS upload_date,
                                    DocUpdateUser AS update_user,
                                    DATE(DocUpdateDate) AS update_date,
                                    CASE
                                        WHEN BibTitle = '' THEN null
                                        ELSE BibTitle
                                    END AS title,
                                    CAST(BibYear AS STRING) AS year_published,
                                    CASE
                                        WHEN BibSource = '' THEN null
                                        ELSE BibSource
                                    END AS publication,
                                    BibVolume AS volume,
                                    CAST(BibIssue AS INTEGER) AS issue,
                                    CAST(BibNumero AS INTEGER) AS numero,
                                    CAST(BibFirstPage AS INTEGER) AS first_page,
                                    CAST(BibLastPage AS INTEGER) AS last_page,
                                    CAST(ContPageCount AS INTEGER) AS num_pages,
                                    CAST(ContTreatCount AS INTEGER) AS num_treatments,
                                    CAST(ContTreatCitCount AS INTEGER) AS num_treat_citations,
                                    CAST(ContMatCitCount AS INTEGER) AS num_mat_citations,
                                    CAST(ContFigCount AS INTEGER) AS num_figures,
                                    CAST(ContTabCount AS INTEGER) AS num_tables,
                                    CAST(ContBibRefCitCount AS INTEGER) AS num_references
                    FROM `%s.apis.tb_diostats_publications_stats`
                    WHERE DocUpdateDate BETWEEN '%s' AND '%s'
            """,
                IFNULL(VAR_SANDBOX_ID, 'poc-plazi-raw'),
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
                    INSERT INTO `%s.%s.%s`
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