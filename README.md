# Simple Quick Start Guide DfT-Street-Manager-Pipeline

> [!NOTE]  
> **This project is under active construction**. 
It currently only processes Street Manager Permit data. Section 58 & Activity Data coming soon. 

- **Permit Data**: ✅

- **Section 58 Data**: 🚫

- **Activity Data**: 🚫


> [!IMPORTANT]
> This is only meant as a quick start guide!
> You will need to complete a few extra steps before being able to fully deploy this pipeline.

This repository contains an ETL pipeline for processing DfT Street Manager Open Data. 

Follow the steps below to clone and set up the project.

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
Create an [AWS account]("https://aws.amazon.com") if you don't have one already.
In the AWS Secrets Manager console, create a new secret to store your environment variables. Update the code to retrieve the secrets using the AWS SDK (boto3). 

Alternatively, you can use a different method to manage environment variables, such as a .env file or another secrets management tool.

### 5. Set Up MotherDuck
Sign up for a MotherDuck account at [MotherDuck]("https://motherduck.com").
Obtain the necessary credentials and connection details for your MotherDuck instance - MotherDuck Token, for example.

Update the code to connect to MotherDuck using the provided credentials.

### 6. Run the Pipeline

>[!NOTE]
> For the most recent permit data use "monthly_main.py" for historic data use "historic_main.py".  

```bash
python src/monthly_main.py

or 

python src/historic_main.py
```

This will execute the ETL pipeline to process the DfT Street Manager Open Data and load it into MotherDuck for further processing and/or analytics. 