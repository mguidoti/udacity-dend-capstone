CREATE TABLE IF NOT EXISTS `poc-plazi-raw.apis.notion_batch_db`
(
    Name                    STRING      OPTIONS(description="Name of the batch process."),
    Status                  STRING      OPTIONS(description="Status of the batch process."),
    Creator                 STRING      OPTIONS(description="Creator of the batch process."),
    Counter                 STRING      OPTIONS(description="Person responsible for counting errors found in this process."),
    QCer                    STRING      OPTIONS(description="Person responsible for QCing this particular publication."),
    Batch_Type              STRING      OPTIONS(description="Type of the given batch. Could be `development`, `testing`, `pre-production`, `local-production`, `ready`, `no scraper` and `production`."),
    A_Date_Start_End        STRING      OPTIONS(description="Start and end date. To be automatically filled."),
    A_Template_Used         STRING      OPTIONS(description="Templated used. To be automatically filled."),
    A_Docs_Processed        STRING      OPTIONS(description="Docs processed. To be automatically filled."),
    Updated                 DATETIME    OPTIONS(description="Last update date."),
)
PARTITION BY DATE(Updated)
CLUSTER BY Creator, Counter, QCer
OPTIONS (
    description="Raw table with the data from the Batch DB in Plazi's instance of Notion."
)