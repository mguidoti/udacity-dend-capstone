CREATE TABLE IF NOT EXISTS `poc-plazi-raw.apis.tb_ephstats_stats`
(
    DocDocId                    STRING      OPTIONS(description='Protocol UUID.'),
    DocSubjectDocId             STRING      OPTIONS(description='Document UUID.'),
    DocUpdateUser               STRING      OPTIONS(description='User to Create Error Protocol.'),
    DocUpdateDate               DATE        OPTIONS(description='Date of Error Protocol Creation.'),
    DocUpdateYear               STRING      OPTIONS(description='Year of Error Protocol Creation.'),
    DocUpdateMonth              STRING      OPTIONS(description='Month of Error Protocol Creation.'),
    DocPrevDocId                STRING      OPTIONS(description='Replaces Error Protocol UUID.'),
    DocNextDocId                STRING      OPTIONS(description='Replaced by Error Protocol UUID.'),
    DocErrors                   STRING      OPTIONS(description='Number of Errors.'),
    DocErrorsRemoved            STRING      OPTIONS(description='Number of Errors Fixed.'),
    DocFalsePos                 STRING      OPTIONS(description='Number of False Positives.'),
    DocFalsePosAdded            STRING      OPTIONS(description='Number of Errors Marked as False Positives.'),
    DocErrorsBlocker            STRING      OPTIONS(description='Number of Blockers.'),
    DocErrorsRemovedBlocker     STRING      OPTIONS(description='Number of Blockers Fixed.'),
    DocFalsePosBlocker          STRING      OPTIONS(description='Number of False Positive Blockers.'),
    DocFalsePosAddedBlocker     STRING      OPTIONS(description='Number of Blockers Marked as False Positives.'),
    DocErrorsCritical           STRING      OPTIONS(description='Number of Criticals.'),
    DocErrorsRemovedCritical    STRING      OPTIONS(description='Number of Criticals Fixed.'),
    DocFalsePosCritical         STRING      OPTIONS(description='Number of False Positive Criticals.'),
    DocFalsePosAddedCritical    STRING      OPTIONS(description='Number of Criticals Marked as False Positives.'),
)
PARTITION BY DocUpdateDate
CLUSTER BY DocSubjectDocId, DocUpdateUser
OPTIONS (
    description="Raw table with the data on errors from the quality control process."
)