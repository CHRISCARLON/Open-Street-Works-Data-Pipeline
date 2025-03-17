# Open Street Works Data Pipeline 🚧

> [!IMPORTANT]
> This is currently being rewritten to use a more modular and flexible approach.
>
> I wrote this project a while ago and it's time to update the code to be more maintainable and flexible.
>
> I'll update this README properly when the new version is ready.

[![codecov](https://codecov.io/github/CHRISCARLON/Open-Street-Works-Data-Pipeline/branch/new-data-dev-branch/graph/badge.svg?token=T4PLSPAXDE)](https://codecov.io/github/CHRISCARLON/Open-Street-Works-Data-Pipeline)

**Example pipeline processing Street Manager permit data, Ordnance Survey Open USRN Data, and Geoplace SWA Code data.**

![StreetManagerPipeline](https://github.com/user-attachments/assets/b169f3b3-64bf-4129-9021-135a56726d3a)

**This is a an end to end analytical pipeline that ingests 3 data sources, performs the relevant transformations, and runs the required DBT models for analytics - it takes around 40 minutes to run.**

**This pipeline feeds an Evidence dashboard that is updated monthly - it can be found here: [Word on The Street](https://word-on-the-street.evidence.app)**

## Overview

**This repository contains an efficient data pipeline for processing and analysing:**

1. DfT's Street Manager archived permit data

2. Ordnance Survey's Open USRN data

3. Geoplace's SWA Code data

4. Scottish Road Works Register (SRWR) archived permit data (TBC)

## Open Street Works Data Pipeline in 3 points

> [!NOTE]
> The aim of this project is simple.
>
> Reduce the time it takes to deliver value from open street works data.

**It's fast**

- Process an entire month of Street Manager archived permit data ready for analysis in 5 minutes.
- Process an entire year of Street Manager archived permit data ready for anylsis in around 1 hour.
- Process all Street Manager archived pemit data from 2020 to 2024 in the morning and be ready to analyse the data in the afternoon.
- The pipeline utilises batch processing so no need to download, unzip, and deal with saving files to disk - everything is kept in memory.

**It's not fussy**

- Run it where you want.
- Run it locally with Docker or a Python Venv if you want.
- Run it on a Google Cloud Function/AWS Lambda Function (with some caveats).
- Run it as Fargate Task on AWS or a Google Compute Engine.

**It's flexible**

- The project is modular so you can customise it to fit your own needs.
- Don't want to use AWS Secrets Manager for environment variables? Use another provider or a simple .env file (recommended for local dev only).
- Don't want to use MotherDuck as your data warehouse? Add in a function so the end destination is Google Big Query instead.
- Only want to focus on Street Manager data? Launch the entry point that doesn't process SRWR data.
- You can integrate other tools from the Modern Data Stack such as DLT, DBT, or orchestrators like Airflow and Mage if you want more functionality.
- You can run several instances of the project for different analytical requirements.

## Why did I create this Project?

Both DfT's Street Manager and Scotland's SRWR are the authoritative sources of street work permit data for England and Scotland.

They make available large quantities of archived permit data every month and have done so for several years.

Both data sources also need to be combined with other data sources for meaningful analysis.

This equates to a lot of data and processing it can be slow and painful if you're not careful.

**This project can help you:**

- Maintain a consistent and structured way to develop, test, and deploy street work permit data pipelines.
- Automate your development and deployment so you can focus on analysis and delivering value from the data.
- Utilise the power of cloud compute to process data faster.
- Utilise elements of the Modern Data Stack that allow for slick reporting and BI.

## Impact Scores Model

I currently use this pipeline to generate a monthly street works impact score for each USRN in England

## Overview

This model calculates and normalises impact scores for road works across England's highway network.

It combines permit data with traffic and infrastructure metrics to produce weighted impact scores that reflect both the direct impact of works and the broader network.

## Input Data Sources

1. **Permit Data**

   - In-progress works (`in_progress_list_england`)
   - Completed works (`completed_list_england`)
   - Key fields: USRN, street name, highway authority, work category, TTRO requirements, traffic sensitivity, traffic management type

2. **Infrastructure Data**
   - UPRN-USRN mapping (`uprn_usrn_count`)
   - DFT Local Authority data (`dft_la_data_latest`) contains road length and traffic flow information

## Impact Score Calculation

### Base Impact Factors

- **Work Category Impact** (0-5 points)

  - Major works: 5 points
  - Immediate works: 4 points
  - Standard works: 2 points
  - Minor works: 1 point
  - etc

- **Additional Impact Factors**
  - TTRO Required: +0.5 points
  - Traffic Sensitive: +0.5 points
  - Traffic Management Impact: +0-2 points based on severity
  - UPRN Density Impact: +0.2-1.6 points based on UPRN point density on a USRN

### Network Context Adjustment

The model applies a network importance factor based on:

- Total road length in the authority
- Traffic flow data (2023)
- Traffic density per km (length/flow)
- Normalised network importance factor (0-1 scale)

## Output

The final model produces a table with:

- Location identifiers (USRN, street name, highway authority)
- Raw impact scores
- Network metrics (road length, traffic flow, density)
- Final weighted impact scores that account for both direct works impact and network importance

This model helps identify high-impact works areas by considering both the immediate disruption of works and their context within the broader road network.
