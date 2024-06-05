# Monitoring Germany's Core Energy System Dataset: A Data Quality Analysis of the Marktstammdatenregister
This is the github repository of the paper [Monitoring Germany's Core Energy System Dataset: A Data Quality Analysis of the Marktstammdatenregister](https://arxiv.org/abs/2304.10581).
The dashboards are live at https://marktstammdaten.kotthoff.dev/

## Getting Started

1. Create a conda environment called `verify-marktstammdaten` and activate it
1. Run `pip install -r requirements.txt`
1. Run `datasette install datasette-dashboards`
1. Run `docker-compose up`
1. Run `./run_tests.sh`
1. Run `datasette serve .`
