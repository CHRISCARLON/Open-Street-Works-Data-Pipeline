# Dockerfile for pipeline deployment
# This is currently set up to deloy on AWS Fargate
FROM --platform=linux/amd64 python:3.11

# Install system dependencies
RUN apt-get -y update

RUN apt-get install -y \
    libgdal-dev \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements file
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY dbt ./dbt

RUN chmod +x ./dbt/street_manager_street_works_analysis/run_dbt_jobs.sh

CMD ["sh", "-c", "python ./src/main.py && cd ./dbt/street_manager_street_works_analysis && ./run_dbt_jobs.sh"]
