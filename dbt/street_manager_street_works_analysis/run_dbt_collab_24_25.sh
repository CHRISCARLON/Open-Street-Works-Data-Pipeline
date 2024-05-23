#!/bin/bash

# Set this to run after a monthly permit pipeline to automate the creation of analysis tables

# Run dbt run with the specified model
dbt run --models aggregated_collab_works_24_25 --vars "{motherduck_token: $MOTHERDUCK_TOKEN, motherduck_db_name: $MOTHERDB}"