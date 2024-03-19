# verify-marktstammdaten
Verifying and testing data in the German Energy dataset "Marktstammdatenregister"

https://marktstammdaten.kotthoff.dev/

## Getting Started

1. Create a conda environment called `verify-marktstammdaten` and activate it
1. Run `pip install -r requirements.txt`
1. Run `datasette install datasette-dashboards`
1. Run `docker-compose up`
1. Run `./run_tests.sh`
1. Run `datasette serve -m .\metadata.yml .\failed_tests.db`
