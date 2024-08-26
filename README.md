# Open Street Works Data Pipeline

[![codecov](https://codecov.io/github/CHRISCARLON/Open-Street-Works-Data-Pipeline/branch/new-data-dev-branch/graph/badge.svg?token=T4PLSPAXDE)](https://codecov.io/github/CHRISCARLON/Open-Street-Works-Data-Pipeline)

## Overview

**This repository contains an efficient data pipeline for processing:**

1. DfT's Street Manager archived permit data

2. Ordnance Survey's Open USRN data

3. Geoplace's SWA Code list

4. Scottish Road Works Register (SRWR) archived permit data (TBC)

**The goal is to create a flexible, easily deployable pipeline to produce insights from street work data**

### Open Street Works Data Pipeline in 3 points:

>[!NOTE]
> The aim of this project is simple.
>
> Reduce the time it takes to deliver value from open street works data.

1. **It's fast**
- Process an entire month of Street Manager archived permit data ready for analysis in 5 minutes.
- Process an entire year of Street Manager archived permit data ready for anylsis in around 1 hour.
- Process all Street Manager archived pemit data from 2020 to 2024 in the morning and be ready to analyse the data in the afternoon.
- The pipeline utilises batch processing so no need to download, unzip, and deal with saving files to disk - everything is kept in memory.

2. **It's not fussy**
- Run it where you want.
- Run it locally with Docker or a Python Venv if you want.
- Run it on a Google Cloud Function/AWS Lambda Function (with some caveats).
- Run it as Fargate Task on AWS or a Google Compute Engine.

3. **It's flexible**
- The project is modular so you can customise it to fit your own needs.
- Don't want to use AWS Secrets Manager for environment variables? Use another provider or a simple .env file (recommended for local dev only).
- Don't want to use MotherDuck as your data warehouse? Add in a function so the end destination is Google Big Query instead.
- Only want to focus on Street Manager data? Launch the entry point that doesn't process SRWR data.
- You can integrate other tools from the Modern Data Stack such as DLT, DBT, or orchestrators like Airflow and Mage if you want more functionality.
- You can run several instances of the project for analytical requirments.

### Why did I create this Project?

Both DfT's Street Manager and Scotland's SRWR are the authoritative sources of street work permit data for England and Scotland.

They make available large quantities of archived permit data every month and have done so for several years.

This equates to a lot of data and processing it can be slow and painful if you're not careful.

**This project can help you:**

- Maintain a consistent and structured way to develop, test, and deploy street work permit data pipelines.
- Automate your development and deployment so you can focus on analysis and delivering value from the data.
- Utilise the power of Cloud Compute to process data faster.
- Utilise elements of the Modern Data Stack to allow for easy Dashboarding.
