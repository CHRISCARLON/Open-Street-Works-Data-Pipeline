#!/bin/bash
# Set this to run after a monthly permit pipeline to automate the creation of analysis tables

# Load environment variables from .env file
set -a
source .env
set +a

# Run dbt run with the specified model
dbt run --models aggregated_collab_works_24_25