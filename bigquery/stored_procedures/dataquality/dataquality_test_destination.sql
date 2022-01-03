CREATE OR REPLACE PROCEDURE `data-tools-prod-336318.dataquality.test_destination`(VAR_PROJECT_ID STRING,
                                                                                  VAR_DATASET STRING,
                                                                                  VAR_TABLE STRING,
                                                                                  VAR_CONFIG_EMPTY_CHECK BOOL,
                                                                                  VAR_CONFIG_DUPLICATES_CHECK BOOL,
                                                                                  VAR_CONFIG_DUPLICATES_TABLE_KEY ARRAY<STRING>)

-- ################################################################################
-- ### Object: data-tools-prod-336318.dataquality.on_destination
-- ### Created at: 2021-12-28
-- ### Description: Data Quality process to check the destination table
-- ### Author: Marcus Guidoti | Plazi's Data Engineer
-- ### Email: guidoti@plazi.org

-- ### Change Log:
-- ################################################################################

/* EXAMPLE OF USE:
CALL `data-tools-prod-336318.dataquality.on_destination`('poc-plazi-raw', 'apis', 'tb_publications', true, true, ['DocArticleUuid', 'DocName'])
----------
VAR_PROJECT_ID = 'poc-plazi-raw'
VAR_DATASET = 'apis'
VAR_TABLE = 'tb_publications'
VAR_CONFIG_EMPTY_CHECK = true # boolean field to turn on/off this specific checker
VAR_CONFIG_DUPLICATES_CHECK = true # boolean field to turn on/off this specific checker
VAR_CONFIG_DUPLICATES_TABLE_KEY = ['DocArticleUuid', 'DocName'] # table key for uniqueness
*/

BEGIN

    DECLARE VAR_EMPTY_CHECK_RESULTS BOOL DEFAULT NULL;
    DECLARE VAR_DUPLICATES_CHECK_RESULTS BOOL DEFAULT null;

    -- Start of Empty Checker
    BEGIN

        -- Checks if the checker was turned off
        IF VAR_CONFIG_EMPTY_CHECK = false THEN
        EXECUTE IMMEDIATE"""
            SELECT true;
        """
        INTO VAR_EMPTY_CHECK_RESULTS;

        -- If not then consult __TABLES__ for the provided project and dataset
        -- in order to return the total number of rows without having to read the
        -- entire destination table
        ELSEIF VAR_CONFIG_EMPTY_CHECK = true THEN
            EXECUTE IMMEDIATE FORMAT("""
                SELECT row_count > 0
                FROM `%s.%s.__TABLES__`
                WHERE table_id = '%s'
            """,
                VAR_PROJECT_ID,
                VAR_DATASET,
                VAR_TABLE
            )
            INTO VAR_EMPTY_CHECK_RESULTS;
        END IF;

        -- It falls here if the table provided doesn't exist
        IF VAR_EMPTY_CHECK_RESULTS IS NULL THEN
            RAISE USING MESSAGE = """dataquality.on_destination - empty check - ERROR (01): Table """ || VAR_PROJECT_ID || """.""" || VAR_DATASET || """.""" || VAR_TABLE || """ doesn't exist.""";
        -- And here, if the table provided is indeed empty
        ELSEIF VAR_EMPTY_CHECK_RESULTS = false THEN
            RAISE USING MESSAGE = """dataquality.on_destination - empty check - ERROR (02): Table """ || VAR_PROJECT_ID || """.""" || VAR_DATASET || """.""" || VAR_TABLE || """ is empty.""";
        END IF;
    END;

    -- Start of Duplicates Checker
    BEGIN

        -- Checks if the checker was turned off
        IF VAR_CONFIG_DUPLICATES_CHECK = false THEN
        EXECUTE IMMEDIATE"""
            SELECT false;
        """
        INTO VAR_DUPLICATES_CHECK_RESULTS;

        -- If not then consult the provided table in order to find duplicates based
        -- on the table key provided in the VAR_CONFIG_DUPLICATES_TABLE_KEY
        ELSEIF VAR_CONFIG_DUPLICATES_CHECK = true THEN
            EXECUTE IMMEDIATE FORMAT("""
                SELECT COUNT(*) > 0
                FROM (SELECT %s,
                             COUNT(*)
                      FROM `%s.%s.%s`
                      GROUP BY %s
                      HAVING COUNT(*) > 1)
            """,
                ARRAY_TO_STRING(VAR_CONFIG_DUPLICATES_TABLE_KEY, ', '),
                VAR_PROJECT_ID,
                VAR_DATASET,
                VAR_TABLE,
                ARRAY_TO_STRING(VAR_CONFIG_DUPLICATES_TABLE_KEY, ', ')
            )
            INTO VAR_DUPLICATES_CHECK_RESULTS;
        END IF;

        -- Second check, if needed, for the empytness of the table
        -- This check is avoided in case the metadata was already updated into __TABLES__
        IF VAR_EMPTY_CHECK_RESULTS = false THEN
            EXECUTE IMMEDIATE FORMAT("""
                SELECT COUNT(*) > 0
                FROM `%s.%s.%s`
            """,
                VAR_PROJECT_ID,
                VAR_DATASET,
                VAR_TABLE
            )
            INTO VAR_EMPTY_CHECK_RESULTS;
        END IF;

        -- It falls here if something unpredictable has happened and the resulting variable is still set to NULL
        IF VAR_DUPLICATES_CHECK_RESULTS IS NULL THEN
            RAISE USING MESSAGE = """dataquality.on_destination - duplicates check - ERROR (02): Something happened when checking for duplicates in table `""" || VAR_PROJECT_ID || """.""" || VAR_DATASET || """.""" || VAR_TABLE || """`.""";
        -- And here if the table indeed has duplicates
        ELSEIF VAR_DUPLICATES_CHECK_RESULTS = true THEN
            RAISE USING MESSAGE = """dataquality.on_destination - duplicates check - ERROR (01): Table `""" || VAR_PROJECT_ID || """.""" || VAR_DATASET || """.""" || VAR_TABLE || """` has duplicates. Table key provided: """ || ARRAY_TO_STRING(IFNULL(VAR_CONFIG_DUPLICATES_TABLE_KEY, ['']), ', ') || """.""";
        END IF;
    END;
END;
