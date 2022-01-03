CREATE OR REPLACE PROCEDURE `data-tools-prod-336318.dataquality.test_source`(VAR_DAG_ID STRING,
                                                                             VAR_TASK_ID STRING,
                                                                             VAR_AIRFLOW_TOLERANCE_DAYS INT64,
                                                                             VAR_CONFIG_AIRFLOW_CHECK BOOL,
                                                                             VAR_AIRFLOW_ALLOWED_STATES ARRAY<STRING>)

-- ################################################################################
-- ### Object: data-tools-prod-336318.dataquality.on_source
-- ### Created at: 2021-12-28
-- ### Description: Data Quality process to check the Airflow Log Table
-- ### Author: Marcus Guidoti | Plazi's Data Engineer
-- ### Email: guidoti@plazi.org

-- ### Change Log:
-- 2021-12-28: Changed variable types VAR_AIRFLOW_TOLERANCE_DAYS from STRING to
-- INT64, and VAR_CONFIG_AIRFLOW_CHECK from STRING to BOOL, modifying code accordingly
-- ################################################################################

/* EXAMPLE OF USE:
CALL `data-tools-prod-336318.dataquality.on_sources`('testing_tb_function_to_raw', 'function_tb_api_treatments_stats', 0, true, ['success', 'skipped'])
----------
VAR_DAG_ID = 'testing_tb_function_to_raw'
VAR_TASK_ID = 'function_tb_api_treatments_stats'
VAR_AIRFLOW_TOLERANCE_DAYS = 0 # meaning, the same day, and -1 means the day before
VAR_CONFIG_AIRFLOW_CHECK = true # boolean field, anything else will throw an exception
VAR_AIRFLOW_ALLOWED_STATES = ['success', 'skipped'] # It's possible to pass a single value, as long as in an array
*/

BEGIN

    DECLARE VAR_AIRFLOW_CHECK_RESULTS BOOL DEFAULT null;

    BEGIN
        -- Checks if the checker was turned off
        IF VAR_CONFIG_AIRFLOW_CHECK = false THEN
            EXECUTE IMMEDIATE"""
                SELECT true;
            """
            INTO VAR_AIRFLOW_CHECK_RESULTS;

        -- If not, and if the checker is on, then consult `data-tools-prod-336318.logs.airflow`
        -- with the provided parameters and check the status of the latest dag run available
        ELSEIF VAR_CONFIG_AIRFLOW_CHECK = true THEN
            EXECUTE IMMEDIATE"""
                SELECT IF (
                    DATE_DIFF(CURRENT_DATE(), DATE(start_date), DAY) <= @tolerance_in_days AND
                    state IN UNNEST(@allowed_states),
                    TRUE,
                    FALSE
                )
                FROM `data-tools-prod-336318.logs.airflow`
                WHERE dag_id = @dag_id
                AND task_id = @task_id
                -- Here I'm making sure that I'm retrieving only the latest dag run available
                AND start_date = (SELECT MAX(start_date)
                                FROM `data-tools-prod-336318.logs.airflow`
                                WHERE dag_id = @dag_id
                                AND task_id = @task_id);
            """
            INTO VAR_AIRFLOW_CHECK_RESULTS
            USING VAR_AIRFLOW_TOLERANCE_DAYS AS tolerance_in_days,
                VAR_AIRFLOW_ALLOWED_STATES AS allowed_states,
                VAR_DAG_ID AS dag_id,
                VAR_TASK_ID AS task_id;

        -- It falls here if the VAR_CONFIG_AIRFLOW_CHECK provided isn't allowed. I believe this won't be the case since I'm forcing the type BOOL for the checker,
        -- and as such, can wither be TRUE or FALSE. This error will cause the dataquality airflow task to fail
        ELSE
            RAISE USING MESSAGE = """dataquality.on_source - airflow check - ERROR (04): VAR_CONFIG_AIRFLOW_CHECK isn't recognizable. Please set to 'na' or 'airflow', not '""" || VAR_CONFIG_AIRFLOW_CHECK || """'.""";
        END IF;

        -- Check the result of the checker. If NULL, means that nothing was found on `data-tools-prod-336318.logs.airflow`. This error will cause the dataquality airflow task to fail
        IF VAR_AIRFLOW_CHECK_RESULTS IS NULL THEN
            RAISE USING MESSAGE = """dataquality.on_source - airflow check - ERROR (02): Provided parameters returned NULL, please, verify them first by manually querying them in `data-tools-prod-336318.logs.airflow`.""";
        -- If not NULL and if FALSE, then it means that it returned false. This error will cause the dataquality airflow task to fail
        ELSEIF VAR_AIRFLOW_CHECK_RESULTS = false THEN
            RAISE USING MESSAGE = """dataquality.on_source - airflow check - ERROR (03): Checker returned FALSE to the provided states (""" || ARRAY_TO_STRING(VAR_AIRFLOW_ALLOWED_STATES, ', ') ||  """) and tolerance days (""" || VAR_AIRFLOW_TOLERANCE_DAYS || """).""";
        END IF;
    END;
END;