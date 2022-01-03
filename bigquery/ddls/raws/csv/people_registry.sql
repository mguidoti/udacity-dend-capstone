CREATE TABLE IF NOT EXISTS `poc-plazi-raw.csv.people_registry`
(
    Name                    STRING      OPTIONS(description="Name of the person."),
    Institutions            STRING      OPTIONS(description="Institution entry in Notion's Database `Institutions`, where the person is affiliated."),
    Departments             STRING      OPTIONS(description="Department where the person is affiliated in the instutition from the previous field."),
    Institution             STRING      OPTIONS(description="Name of the institution of affiliation."),
    Institutional_Roles     STRING      OPTIONS(description="Role of the person within the institution where she/he is affiliated."),
    Interaction_Roles       STRING      OPTIONS(description="Role played by the person in interaction with Plazi."),
    Workload                STRING      OPTIONS(description="Current workload, calculated automatically in Notion. Not automatically updated here since this base is imported by CSV."),
    Open_Tasks              STRING      OPTIONS(description="Quantity of open tasks, from tasks assigned in Notion."),
    Productivity_Dashboard  STRING      OPTIONS(description="Link to the productivity dashboard of the person, usually in ClickUp."),
    Google_Drive            STRING      OPTIONS(description="Google Drive link to a folder related to this given person."),
    Contract                STRING      OPTIONS(description="Link to a copy of the contract of this given person."),
    Time_Off                STRING      OPTIONS(description="Current time off/vacations, start to end."),
    Timezone                STRING      OPTIONS(description="Timezone where the person is currently located."),
    Affiliation             STRING      OPTIONS(description="Affiliation string, to be used in publications and etc."),
    Email                   STRING      OPTIONS(description="Email of contact of the given person."),
    ORCID                   STRING      OPTIONS(description="ORCID of the given person."),
    GitHub                  STRING      OPTIONS(description="GitHub username of the given person."),
    Skype                   STRING      OPTIONS(description="Skype account of the given person."),
    Decisions               STRING      OPTIONS(description="Links to Decisions Notion's Database, from Plazi's instance, where the person participated."),
    Full_Name               STRING      OPTIONS(description="Full name of the person."),
    Emergency_Contact       STRING      OPTIONS(description="Name of the emergency contact of the person."),
    EC_Email                STRING      OPTIONS(description="Emergency contact email."),
    EC_Phone                STRING      OPTIONS(description="Emergency contact phone number."),
    Projects                STRING      OPTIONS(description="Projects associated with the person, with links to the page of the project in Notion's Projects Database."),
    Campaigns               STRING      OPTIONS(description="Campaigns associated with the person, with links to the page of the campaign in Notion's Campaigns Database."),
    Tasks                   STRING      OPTIONS(description="Tasks associated with the person, with links to the page of the task in Notion's Tasks Database."),
    Events                  STRING      OPTIONS(description="Events associated with the person, with links to the page of the event in Notion's Events Database."),
    Talks_Leading_Author    STRING      OPTIONS(description="Talks in which the person is the leading author, with links to the page of the talk in Notion's Talks Database."),
    Talks_Coauthor          STRING      OPTIONS(description="Talks in which the person isa co-author, with links to the page of the talk in Notion's Talks Database."),
    Discussions             STRING      OPTIONS(description="Links to Discussions Notion's Database, from Plazi's instance, where the person participated."),
    Count_Total_Tasks       STRING      OPTIONS(description="Total number of tasks assigned to the person in Notion."),
    Priority_Count          STRING      OPTIONS(description="Total number of tasks defined as priority assigned to the person in Notion."),
    Address                 STRING      OPTIONS(description="Full address of the person."),
    Meetings                STRING      OPTIONS(description="Links to Meetings Notion's Database, from Plazi's instance, where the person participated."),
    Off_Time                STRING      OPTIONS(description="Events where the person was out-of-office (e.g., medical appointments)."),
    Reimbursements          STRING      OPTIONS(description="Links to the reembursements pages, with the invoices issued by the person, in the Plazi's instance of Notion, Reembursement Database."),
    Related_to_Hardware_DB  STRING      OPTIONS(description="Hardware currently in hold by the person."),
    Hourly_Rate             STRING      OPTIONS(description="Hourly rate of the given person.")

)
CLUSTER BY Institution
OPTIONS (
    description="Raw table containing information from Plazi's instance of Notion, `People` Database."
)