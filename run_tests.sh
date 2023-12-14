#!/bin/bash

conda activate verify-marktstammdaten # this environment needs to have the needed packages installed

cd dbt

dbt run

dbt test --store-failures

cd ..

python failed_tests_to_sqlite.py
