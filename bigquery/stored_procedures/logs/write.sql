CREATE OR REPLACE PROCEDURE `data-tools-prod-336318.logs.write`(VAR_LOG_TABLE STRING,
                                                                VAR_SERVICE STRING,
                                                                VAR_PROCESS STRING,
                                                                VAR_LOAD_MODE STRING,
                                                                VAR_ZONE STRING,
                                                                VAR_PROJECT_ID STRING,
                                                                VAR_DATASET STRING,
                                                                VAR_TABLE STRING,
                                                                VAR_START_PROCESS_DATE DATE,
                                                                VAR_STATUS STRING,
                                                                VAR_RESPONSE STRING,
                                                                VAR_MESSAGE STRING,
                                                                VAR_NUM_ROWS_INSERTED INT64,
                                                                VAR_LOGGED_FROM STRING,
                                                                VAR_RAN_AT DATETIME)

-- ################################################################################
-- ### Object: data-tools-prod-336318.logs.write
-- ### Created at: 2021-12-28
-- ### Description: Stored Procedure to record log messages from other Stored
-- ###              Procedures into `data-tools-prod-336318.log.data_updates`
-- ### Author: Marcus Guidoti | Plazi's Data Engineer
-- ### Email: guidoti@plazi.org

-- ### Change Log:
-- ################################################################################

/* EXAMPLE OF USE:
CALL `data-tools-prod-336318.logs.write`('data_updates', 'BigQuery', 'load_trd_treatment_bank_publications_basic_stats', 'DELTA', 'RAW', 'poc-plazi-trusted', 'treatment_bank', 'publications_basic_stats', '2021-12-28', 'success', '', 'Daily load successfully completed.', 500, 'BigQuery', CURRENT_DATE())
----------
VAR_SERVICE = 'BigQuery'
VAR_PROCESS = 'load_trd_treatment_bank_publications_basic_stats',
VAR_LOAD_MODE = 'DELTA',
VAR_ZONE = 'RAW',
VAR_PROJECT_ID = 'poc-plazi-trusted',
VAR_DATASET = 'treatment_bank',
VAR_TABLE = 'publications_basic_stats',
VAR_DATE = '2021-12-28',
VAR_STATUS = 'success',
VAR_RESPONSE = '',
VAR_MESSAGE = 'Daily load successfully completed.',
VAR_NUM_ROWS_INSERTED = 500,
VAR_LOGGED_FROM = 'BigQuery',
VAR_RAN_AT = CURRENT_DATE()
*/

BEGIN

    EXECUTE IMMEDIATE"""
        INSERT INTO `data-tools-prod-336318.logs.""" || VAR_LOG_TABLE || """`
            VALUES (
                @service,
                @process,
                @load_mode,
                @zone,
                @project_id,
                @dataset,
                @table,
                @start_process_date,
                @status,
                @response,
                @message,
                @num_rows_inserted,
                @logged_from,
                @ran_at
            )"""
            USING VAR_SERVICE AS service,
                  VAR_PROCESS AS process,
                  VAR_LOAD_MODE AS load_mode,
                  VAR_ZONE AS zone,
                  VAR_PROJECT_ID AS project_id,
                  VAR_DATASET AS dataset,
                  VAR_TABLE AS table,
                  VAR_START_PROCESS_DATE AS start_process_date,
                  VAR_STATUS AS status,
                  VAR_RESPONSE AS response,
                  VAR_MESSAGE AS message,
                  VAR_NUM_ROWS_INSERTED AS num_rows_inserted,
                  VAR_LOGGED_FROM AS logged_from,
                  VAR_RAN_AT AS ran_at;
END;