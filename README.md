

<p align="center"><a href="https://www.udacity.com/course/data-engineer-nanodegree--nd027">Udacity Data Engineer Nanodegree</a></p>

# Capstone Project

This project is the final project of Udacity's Data Engineer Nanodegree. I decided to follow my own project, which involves my employer - a swiss-based NGO working to liberate highly-annotated biodiversity-related scientific data to the public, for free. The goal was to provide a proof-of-concept of a data cloud platform, showing that it can be designed to mostly fit free tier quotas while also delivering our needs in terms of data pipelines, analytics and business intelligence applications. As such, the goal assumed was to provide the framework of such data platform, perform some pipelines to load data into the platform from various sources, and create one tip-to-tip pipeline until the final layer of data/product.

Most of the project and questions related the decisions and the processes here contained [are throughfully explained in this link](https://sites.google.com/view/guidoti-udacity-dend-capstone/) - it will be referred several times here.

Additionally, you will find the [applied architectural plans](https://sites.google.com/view/guidoti-udacity-dend-capstone/data-architecture/reference-plans?authuser=2), [the data platform organization](https://sites.google.com/view/guidoti-udacity-dend-capstone/data-architecture/platform-organization), the [data dictionary](https://sites.google.com/view/guidoti-udacity-dend-capstone/data-dictionary), details about the data engineering of this project and even a [firefighter dashboard](https://sites.google.com/view/guidoti-udacity-dend-capstone/firefighter)!

## Summary of Contents

In this repo, you'll find:
- 17 DAGs,
- 1 Custom Operator
- 1 Custom Log lib
- more than 25 table DDLs,
- 8 stored procedures
- 3 applications that runs in Compute Engine VMs,
- 2 generalized Cloud Functions that connect to the source APIs
- Over 2 million rows and...
- ... more than 10k lines of code..!

Once again, more details about the processes here included and somehow in-depth discussion about the decisions, please refer to [this link](https://sites.google.com/view/guidoti-udacity-dend-capstone/).

## Quick Q&A

For more, please, refer to [this link](https://sites.google.com/view/guidoti-udacity-dend-capstone/).

### Data Sources
There are two APIs, one external ([Notion.so](https://developers.notion.com/)) and another one belonging to the same organization ([TreatmentBank](https://tb.plazi.org/GgServer/dioStats)) - but sitting on an on-prem server in Germany. From this particular API, I consumed over 2 million rows.

There was also a CSV import.

Both [architectural reference plans can be found here](https://sites.google.com/view/guidoti-udacity-dend-capstone/data-architecture/reference-plans).


### Why Google Cloud Platform and not AWS
[This is covered here](https://sites.google.com/view/guidoti-udacity-dend-capstone/qa), but in sum, that's because the company already partnered with Google, the free tier seems to fit our needs better and the services (especially BigQuery) too.

### Data Lakehouse?
Well, data lakehouses isn't actually new, and although I do understand the course taught about data warehouses and data lakes, the project's rubric doesn't require specifically to use one or the two of them - so I took the liberty to create something that fits our needs better. Why if fits us better? For several reasons, but mostly, because it fits us better. The idea is to have a single platform to hold unstructured and structure data, a database service that can also process tons of data at low cost (BigQuery) and a free tier big enough to allow this operation for us.

### What about the data modeling?
Our data in this project is in BigQuery, a columnar database - meaning, it's not relational, which means there is no primary keys, or need to apply snowflake data modeling or to be concern about normalization and denormalization.

The data suits our needs almost in a case-by-case situation and instead of having a data modelling per se, [I created data layers, which are explained here](https://sites.google.com/view/guidoti-udacity-dend-capstone/data-architecture/platform-organization).

### Details on the Engineering
[It can all be found here](https://sites.google.com/view/guidoti-udacity-dend-capstone/data-engineering). DAGs are discussed and two particularly complex ones are explained, the processes supporting the POC of the platform, like the [load_params](https://sites.google.com/view/guidoti-udacity-dend-capstone/data-engineering/the-load_params-table?) or the [data quality](https://sites.google.com/view/guidoti-udacity-dend-capstone/data-engineering/data-quality) are discussed in detailed too.

## How to Run
You can create a free account in GCP and use their initial credits to deploy all of this code base into the specific services and test it out. This repository is organized by service, so, it should be fairly easy to replicate my results!

You will only miss access to the Notion.so Databases, as their belong to my private environment, but TreatmentBank API is open.


## Disclaimer
This is a study repo, to fulfil one of the project-requirements of the [Udacity Data Engineer Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027), submitted on January/2022. This has no intention to continue to be developed, and the git commit messages follow the [Udacity Git Style Guide](https://udacity.github.io/git-styleguide/), with the addition of the keyword `file` (used when moving non-coding files).
