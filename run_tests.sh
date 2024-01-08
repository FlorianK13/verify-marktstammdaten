#!/bin/bash

conda activate verify-marktstammdaten # this environment needs to have the needed packages installed

docker-compose up -d

python scripts/load_raw_data.py

cd dbt

dbt run

dbt test --store-failures

cd ..

python scripts/failed_tests_to_sqlite.py

docker-compose down
