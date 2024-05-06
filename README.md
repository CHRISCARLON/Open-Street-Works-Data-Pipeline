# Quickstart Guide for DfT-Street-Manager-Pipeline

**This repository contains an efficient ETL pipeline for processing DfT's Street Manager archived permit data.** 

### DfT-Street-Manager-Pipeline in 3 points:

1. **It's fast!** Process an entire month of archived permit data ready for analysis in around 5 minutes. Process an entire year of archived permit data ready for anylsis in around 1 hour. You could process all archived pemit data from 2020 to 2024 in the morning and be writing SQL queries to analyse the data in the afternoon. All of this will be kept 100% in memory so no need to deal with saving files to disk!. 

2. **It's not fussy!** Run it where you want! Run it locally, on an AWS Lambda Function (with some caveats), or a Google Compute Engine - it's up to you. 

3. **It's flexible!** The project is modular so you can customise it to fit your own needs. Don't want to use AWS Secrets Manager for environment variables? Fine! Use another provider or a .env file. Don't want to use MotherDuck as your data warehouse? Fine! Add in a function so the end destination is Google Big Query instead. 

### Why use this Project?  

Processing Street Manager archived permit data can be slow and painful if you're not careful.  

Here are a few painpoints that I've experienced in the past:

1. The standard Python Zip library can't unzip the type of zip file provided by the Dft [here](https://department-for-transport-streetmanager.github.io/street-manager-docs/archived-notifications/#permit/2024/). 

    This means that:

    - You may need to manually download the file and use WinRAR (Windows) or Utility Arhive (Mac) to unzip it - this can take a while (30 minutes) and adds unwanted delays as well as taking up disk space.     

2. Each month of archived permit data is around 1gb in size and contains around 1+ million individual json files representing individual permit notification records. 

    This means that:

    - Processing times can become slow if you attempt to keep everything in memory without proper batch processing techniques - especially if you're using a library such as Pandas. 

### Please follow the steps below to clone, set up, and run the project.

> [!NOTE]  
> **This project is under active construction**. 
It currently only processes Street Manager Permit data.

- **Permit Data**: ✅

- **Section 58 Data**: 🚫

- **Activity Data**: 🚫

>[!NOTE]
> **Instructions to deploy this ETL pipeline fully to the cloud (GCP Compute Engine) using Terraform are coming soon!** 

> [!IMPORTANT]
> This is only meant as a quickstart guide!
> You will need to complete a few extra steps before being able to fully deploy this pipeline.

### 0. Pre-Requisites 

1. You'll need Python installed locally on your system - I use Python 3.11.
2. You'll need a MotherDuck account.  
3. You'll need an AWS service account - if you want to use the same method as me for storing and retrieving env variables then you'll need to use AWS Secrets Manager. 
4. You'll need to ensure you have the correct environment variables. 

    If you follow my set up you'll need...

    - An AWS secret name (the secrets ID that you chose for your env variables and use to retrieve them).
    - A MotherDuck connection token.
    - A MotherDuck database name. 
    
    If you want to use MotherDuck as your end destination then you'll need to make sure that you have env variables set up that point to the correct database and schemas when you start creating your own tables.

### 1. Clone the Repository

```bash
git clone https://github.com/CHRISCARLON/DfT-Street-Manager-Pipeline.git

cd DfT-Street-Manager-Pipeline
```
This will create a local copy of the repository on your machine.

### 2. Create a Virtual Environment
```bash
python -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install poetry
poetry install
```
Poetry will read the pyproject.toml file and install the required packages into your newly created virtual environment.

### 4. Set Up AWS Secrets Manager
Create an [AWS account](https://aws.amazon.com) if you don't have one already.

If you're not comfortable using the AWS CLI then navigate to the AWS Secrets Manager console in your browser and create a new secret to store your environment variables. 

At runtime, call the below function in the code to retrieve your secrets using the AWS SDK (boto3).

Please note that there are charges when using AWS Secrets Manager - but it's peanuts. 

<img width="808" alt="Screenshot 2024-05-06 at 20 34 00" src="https://github.com/CHRISCARLON/DfT-Street-Manager-Pipeline/assets/138154138/2a99f555-6b8f-4586-8b2f-a7471911bae1">

**Alternatively, you can use a different method to manage environment variables, such as a .env file or another secrets management tool.**

### 5. Set Up MotherDuck
Sign up for a MotherDuck account at [MotherDuck](https://motherduck.com).

Obtain the necessary credentials and connection details for your MotherDuck account - a MotherDuck Token, for example.

You can use the default **my_db** that is provided with your new MotherDuck account, but I recommend that you create a specific database for your permit data. 

Create database schemas for each year - this is where you'll load your data tables for each month of the relevant year. 

For example..

![Screenshot 2024-05-06 at 20 28 45](https://github.com/CHRISCARLON/DfT-Street-Manager-Pipeline/assets/138154138/d98e53ed-983d-41b4-ad17-ac99d1d59a98)

### 6. Run the Pipeline

>[!NOTE]
> For the most recent permit data use **monthly_permit_main.py** and for historic data use **historic_permit_main.py**.  

>[!IMPORTANT]
> Please make sure you have configured everything before running the pipeline. 
> I'd recommend going through the 2 main.py files and become comfortable with them first. 

```bash
python src/monthly_permit_main.py

or 

python src/historic_permit_main.py
```

This will execute the pipeline, process the Street Manager permit data, and then load it into MotherDuck for further processing and/or analytics.

### 7. Analyse the Street Manager permit data and/or perform further transformations

Leverage MotherDuck's serverless execution to run analysis on your permit data and/or normalise it further and create new table relationships. 

I recommend using the permit data in conjunction with Geo Place's [SWA Code list](https://www.geoplace.co.uk/local-authority-resources/street-works-managers/view-swa-codes) - enabling you to build sector level analysis. 
