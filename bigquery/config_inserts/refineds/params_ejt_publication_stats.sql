BEGIN

    -- These ones are defined by the user
    DECLARE VAR_ZONE STRING DEFAULT 'REFINED';
    DECLARE VAR_PROJECT_ID STRING DEFAULT 'poc-plazi-dashboards';
    DECLARE VAR_DATASET STRING DEFAULT 'ejt_v4';
    DECLARE VAR_TABLE STRING DEFAULT 'ejt_publication_stats';
    DECLARE VAR_AUX_DATASET STRING DEFAULT '';
    DECLARE VAR_AUX_TABLE STRING DEFAULT '';
    DECLARE VAR_DELTA_FREQ STRING DEFAULT 'DAILY';
    DECLARE VAR_DELTA_FIELD STRING DEFAULT 'upload_date';
    DECLARE VAR_DELTA_INTERVAL_START INT64 DEFAULT 0;
    DECLARE VAR_DELTA_INTERVAL_END INT64 DEFAULT -1;

    -- This one is used in the script
    DECLARE VAR_CHECK_EXISTENCE BOOL DEFAULT false;

    BEGIN

        EXECUTE IMMEDIATE FORMAT("""
            SELECT true FROM `data-tools-prod-336318.config.load_params`
            WHERE CONCAT(project_id, '.', dataset, '.', table) = '%s.%s.%s'
        """,
            VAR_PROJECT_ID,
            VAR_DATASET,
            VAR_TABLE
        )
        INTO VAR_CHECK_EXISTENCE;

        IF VAR_CHECK_EXISTENCE = true THEN
            EXECUTE IMMEDIATE FORMAT("""
                DELETE FROM `data-tools-prod-336318.config.load_params`
                WHERE project_id = '%s'
                    AND dataset = '%s'
                    AND table = '%s'
            """,
                VAR_PROJECT_ID,
                VAR_DATASET,
                VAR_TABLE
            );
        END IF;

        EXECUTE IMMEDIATE"""
            INSERT INTO `data-tools-prod-336318.config.load_params`
                VALUES(
                    @zone,
                    @project_id,
                    @dataset,
                    @table,
                    @aux_dataset,
                    @aux_table,
                    @delta_freq,
                    @delta_field,
                    @delta_interval_start,
                    @delta_interval_end
                )"""
                USING VAR_ZONE AS zone,
                      VAR_PROJECT_ID AS project_id,
                      VAR_DATASET AS dataset,
                      VAR_TABLE AS table,
                      VAR_AUX_DATASET AS aux_dataset,
                      VAR_AUX_TABLE AS aux_table,
                      VAR_DELTA_FREQ AS delta_freq,
                      VAR_DELTA_FIELD AS delta_field,
                      VAR_DELTA_INTERVAL_START AS delta_interval_start,
                      VAR_DELTA_INTERVAL_END AS delta_interval_end;
    END;
END;