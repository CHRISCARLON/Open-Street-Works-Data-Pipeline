FROM --platform=linux/amd64 python:3.11.3

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY dbt ./dbt

RUN chmod +x ./dbt/street_manager_street_works_analysis/run_dbt_collab_24_25.sh

CMD ["sh", "-c", "python ./src/monthly_permit_main.py && cd ./dbt/street_manager_street_works_analysis && ./run_dbt_collab_24_25.sh"]