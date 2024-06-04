# Open Street Works Data Pipeline
Progress: `█████████████████████████████████████████████████░░░░░` (90%)

Street Manager Monthly Permit Pipeline example: 
![permit-pipeline-data-flow](https://github.com/CHRISCARLON/DfT-Street-Manager-Pipeline/assets/138154138/87b303c8-fa0a-4e6c-8714-cbbce1b7637c)


>[!IMPORTANT]
> Currently working on
> 1. Finalising the Evidence dashboard. 
>
> 2. Integrating Geoplace's SWA Code List.
>
> 3. Speeding up SRWR processing and improving its integration into the project. 


# Quickstart Guide:

**This repository contains an efficient data pipeline for processing:**

1. DfT's Street Manager archived permit data.

2. Scottish Road Works Register (SRWR) archived permit data. 

### Open Street Works Data Pipeline in 3 points:

>[!NOTE]
> The aim of this project is simple... 
>
> Reduce the time it takes to deliver value from open street works data.

1. **It's fast** 
- Process an entire month of Street Manager archived permit data ready for analysis in 5 minutes. 
- Process an entire year of Street Manager archived permit data ready for anylsis in around 1 hour. 
- Process all Street Manager archived pemit data from 2020 to 2024 in the morning and be ready to analyse the data in the afternoon. 
- The pipeline utilises batch processing so no need to download, unzip, and deal with saving files to disk - everything is kept in memory. 

2. **It's not fussy** 
- Run it where you want! 
- Run it locally with Docker or a Python Venv if you want.
- Run it on a Google Cloud Function/AWS Lambda Function (with some caveats). 
- Run it as Fargate Task on AWS or a Google Compute Engine.

3. **It's flexible** 
- The project is modular so you can customise it to fit your own needs. 
- Don't want to use AWS Secrets Manager for environment variables? Fine! Use another provider or a simple .env file (recommended for local dev only). 
- Don't want to use MotherDuck as your data warehouse? Fine! Add in a function so the end destination is Google Big Query instead. 
- Only want to focus on Street Manager data? Fine! Launch the entry point that doesn't process SRWR data.  
- You can integrate other tools from the Modern Data Stack such as DLT, DBT, or orchestrators like Airflow and Mage if you want more functionality. 
- You can run several instances of the project for different purposes.  

### Why use this Project?  

Both DfT's Street Manager and Scotland's SRWR are the authoritative sources of street work permit data for England and Scotland. 

They make available large quantities of archived permit data every month and have done so for several years. 

This equates to a lot of data and processing it can be slow and painful if you're not careful. 

**This project can help you:**

- Maintain a consistent and structured way to develop, test, and deploy street work permit data pipelines. 
- Automate your development and deployment so you can focus on analysis and delivering value from the data. 
- Utilise the power of Cloud Compute to process data faster. 
- Utilise elements of the Modern Data Stack to allow for easy Dashboarding. 


### Please follow the steps below to clone, set up, and run the project.

> [!IMPORTANT]
> This is only meant as a quickstart guide.
> It's not perfect and I'll try to make it more detailed as time goes on :)
>
> You will need to complete a few extra steps before being able to fully use this pipeline so please read the pre-requisites.

# Local Deployment
### 0. Pre-Requisites (if you want to follow the Street Manager Monthly Permit Pipeline example)

1. You'll need Python installed locally on your system - I use Python 3.11.
2. You'll need a MotherDuck account.  
3. You'll need an AWS service account. 
4. You'll need both the AWS CLI and Terraform correctly configured on your local system. 
5. You'll need to ensure that you have the correct environment variables. 

    If you follow my set up you'll need...

    - AWS Secret Manager.
    - A MotherDuck connection token.
    - A MotherDuck database name. 
    
    If you want to use MotherDuck as your end destination then you'll need to make sure that you have env variables set up that point to the correct database and schemas. 

### 1. Clone the Repository

```bash
git clone https://github.com/CHRISCARLON/Open-Street-Works-Data-Pipeline.git

cd Open-Street-Works-Data-Pipeline
```
This will create a local copy of the repository on your machine and take you into it.

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
Poetry will read the pyproject.toml file and install the required packages in your newly created virtual environment.

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

For example... 

![Screenshot 2024-05-06 at 20 28 45](https://github.com/CHRISCARLON/DfT-Street-Manager-Pipeline/assets/138154138/d98e53ed-983d-41b4-ad17-ac99d1d59a98)

### 6. Run the Pipeline

>[!NOTE]
> For the most recent permit data use **monthly_permit_main.py** and for bulk historic data use **historic_permit_main.py**. 
>
>For this example use the most recent permit data. 

>[!IMPORTANT]
> Make sure that you have configured everything before running. 
>
> I'd recommend going through the monthly_permit_main.py file and become comfortable with it first. 
>
> Ensure that your MotherDuck token is accessible at runtime and that you have the correct schemas set up. For local development, you could EXPORT the token via the terminal. 

```bash
python src/monthly_permit_main.py
```
or if you want to use Docker 
```
docker-compose up 
```
This docker-compose.yml is already set up and so is the Dockerfile. 

This will execute the pipeline, process the Street Manager permit data, and then load it into MotherDuck for further processing and/or analytics.

### 7. Analyse the Street Manager permit data and/or perform further transformations

This project leverages the power of DBT to make analysis quick, easy, and scalable. 

If you run this in a Python venv then you will need to run the run_dbt_jobs.sh seperately to run the DBT models. If don't run the DBT models you will only have the raw data tables in  MotherDuck.   

There are 10+ DBT models that automate the creation of different aggregated analysis tables - each one is focused on a different type of insight. 

To run the dbt shell script open up your terminal cd into the DBT folder and run:

```
chmod +x run_dbt_jobs.sh
```
and then 
```
./run_dbt_jobs.sh
```

If you run this in Docker then the DBT models automatically run. 

You can use MotherDuck's UI to run further queries on the data if you'd like to! 

MotherDuck connects into many modern BI tools such as HEX and Preset.  

Check out the videos below from MotherDuck for some cool tutorials on how to build dashboards with data in MotherDuck.

1. https://www.youtube.com/watch?v=F9yHuAO50PQ

2. https://www.youtube.com/watch?v=gemksL8YvOQ 

# Cloud Deployment
>[!NOTE]
> I'm still writing this up!

**It will contain instructions on:**

- Deploying a Docker Container to an AWS ECR (making use the of the handy Makefile!)

- Using Terraform to configure and deploy an AWS Fargate Task on AWS ECS. 
