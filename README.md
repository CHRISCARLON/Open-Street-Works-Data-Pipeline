# Quick Start Guide for DfT-Street-Manager-Pipeline

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

2. Each month of archived permit data is around 1gb in size and contains around 1+ million individual json files representing permit notification records. 

    This means that:

    - Processing times can become slow if you try keep everyting in memory without proper batch processing techniques - especially if you're using a library such as Pandas. 

### Please follow the steps below to clone and set up the project.

> [!NOTE]  
> **This project is under active construction**. 
It currently only processes Street Manager Permit data.

- **Permit Data**: ✅

- **Section 58 Data**: 🚫

- **Activity Data**: 🚫

>[!NOTE]
> **Instructions to deploy to the cloud (GCP) are coming soon!** 


> [!IMPORTANT]
> This is only meant as a quick start guide!
> You will need to complete a few extra steps before being able to fully deploy this pipeline.

### 0. Pre-Requisites 

1. You'll need Python installed locally on your system - I use Python 3.11.
2. You'll need to pip install poetry to install the dependencies.
3. You'll need a MotherDuck account. 
4. You'll need an AWS service account. 

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
Navigate to the AWS Secrets Manager console and create a new secret to store your environment variables. Update the code to retrieve the secrets using the AWS SDK (boto3). Please note that there are charges when using AWS Secrets Manager. 

Alternatively, you can use a different method to manage environment variables, such as a .env file or another secrets management tool.

### 5. Set Up MotherDuck
Sign up for a MotherDuck account at [MotherDuck](https://motherduck.com).
Obtain the necessary credentials and connection details for your MotherDuck account - a MotherDuck Token, for example.

Create a data 

Update the code to connect to MotherDuck using the provided credentials.

### 6. Run the Pipeline

>[!NOTE]
> For the most recent permit data use "monthly_permit_main.py" for historic data use "historic_permit_main.py".  

```bash
python src/monthly_permit_main.py

or 

python src/historic_permit_main.py
```

This will execute the pipeline, process the Street Manager permit data, and then load it into MotherDuck for further processing and/or analytics.