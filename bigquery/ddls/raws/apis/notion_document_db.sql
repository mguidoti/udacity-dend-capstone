CREATE TABLE IF NOT EXISTS `poc-plazi-raw.apis.notion_document_db`
(
    Name                        STRING      OPTIONS(description="Name of the processed file."),
    Status                      STRING      OPTIONS(description="Status of completition of the extraction process."),
    Counter                     STRING      OPTIONS(description="Person responsible for counting errors found in this process."),
    QCer                        STRING      OPTIONS(description="Person responsible for QCing this particular publication."),
    Goal                        STRING      OPTIONS(description="Goal to which this publication belongs."),
    Date_Start                  STRING      OPTIONS(description="Started date of the extraction process of this publication."),
    Created_at_End_Date         DATE        OPTIONS(description="Date where the record was imported into Notion."),
    Template_Name               STRING      OPTIONS(description="Name of the template used in the extraction process."),
    Batch_Process               STRING      OPTIONS(description="Name of the batch process where this extraction process was included."),
    Granularity_Level           STRING      OPTIONS(description="Granalurity level applied to this extraction process."),
    QCed                        STRING      OPTIONS(description="Level to which this extraction process was QCed."),
    Type_of_Doc                 STRING      OPTIONS(description="Type of the processed document. Could be `article`, `errata`, `scientific note` or `monograph`."),
    Nature_of_Doc               STRING      OPTIONS(description="Nature of the processed document. This field will be used in the future in machine learning models. Could be `catalog`, `checklist`, `distribution`, `description` or `review`."),
    Treatments                  STRING      OPTIONS(description="If the processed publication has treatments."),
    DwCA                        STRING      OPTIONS(description="If it generated the DwCA file automatically."),
    Why_no_DwCA                 STRING      OPTIONS(description="The reason why it didn't generate it."),
    Text_Dec_Problem            STRING      OPTIONS(description="If there was any text decoding problem in this extraction process."),
    Fig_Dec_Problem             STRING      OPTIONS(description="If there was any figure decoding problem in this extraction process."),
    Incorrect_Treats            STRING      OPTIONS(description="Number of incorrect marked treatments there was in this extraction process."),
    Missed_Keys                 STRING      OPTIONS(description="Number of missed keys there was in this extraction process."),
    Missed_Treats               STRING      OPTIONS(description="Number of missed treatments there was in this extraction process."),
    Spurious_Treats             STRING      OPTIONS(description="Number of spurious treatments there was in this extraction process."),
    Missed_n_spp                STRING      OPTIONS(description="Number of missed keys there was in this extraction process."),
    Incomplete_Keys             STRING      OPTIONS(description="Number of incomplete keys there was in this extraction process."),
    Missed_Figures              STRING      OPTIONS(description="Number of missed figures there was in this extraction process."),
    Incorrect_Figures           STRING      OPTIONS(description="Number of incorrect figures there was in this extraction process."),
    Fig_Missed_Labels           STRING      OPTIONS(description="Number of missed figured labels there was this extraction process."),
    Incomp_Captions             STRING      OPTIONS(description="Number of incomplete figure captions there was in this extraction process."),
    Missed_Tables               STRING      OPTIONS(description="Number of missed tables there was in this extraction process."),
    Missed_bibRefs              STRING      OPTIONS(description="Number of missed bibliographic references there was in this extraction process."),
    Deg_Miss_treatCits          STRING      OPTIONS(description="Degree to which treatment citations were missed in this extraction process. Could be `innaplicable`, `none`, `low`, `high` or `very high`."),
    Def_Miss_matCits            STRING      OPTIONS(description="Degree to which materials citations were missed in this extraction process. Could be `innaplicable`, `none`, `low`, `high` or `very high`."),
    Suppl_Files                 STRING      OPTIONS(description="Name of supplementary files, if applicable, and separated by comma."),
    Hs_to_DwCA                  STRING      OPTIONS(description="Hours taken to fix errors in order to generate the DwCA file."),
    Hs_Counting_Errors          STRING      OPTIONS(description="Hours taken to count errors."),
    Hs_to_Gran_Level            STRING      OPTIONS(description="Hours taken to adapt the extraction process to the desired granularity level."),
    Hs_Fix_QC_Errors            STRING      OPTIONS(description="Hours taken to fix errors in the desired Quality Control level."),
    UUID                        STRING      OPTIONS(description="UUID of the processed paper."),
    Zenodo_Link                 STRING      OPTIONS(description="Zenodo Link to the publication, on Zenodo."),
    Hs_Fix_ExtQC                STRING      OPTIONS(description="Hours taken to fix errors in the Extended Quality Control process."),
    Hs_PreQC                    STRING      OPTIONS(description="Hours taken to fix errors in a Pre-Quality Control process"),
)
PARTITION BY Created_at_End_Date
CLUSTER BY QCer, Template_Name, Batch_Process
OPTIONS (
    description="Raw table containing information from the Document DB, in Plazi's instance of Notion."
)
