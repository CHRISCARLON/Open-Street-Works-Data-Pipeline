# This is currently set up to deloy on AWS Fargate, remove the linux/amd64 platform for local dev.
FROM --platform=linux/amd64 python:3.11.7

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY dbt ./dbt

RUN chmod +x ./dbt/street_manager_street_works_analysis/run_dbt_jobs.sh

CMD ["sh", "-c", "python ./src/monthly_permit_main.py && cd ./dbt/street_manager_street_works_analysis && ./run_dbt_jobs.sh"]