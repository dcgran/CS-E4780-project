# CS-E4780-project

Course project for the Aalto University course CS-E4780 - Scalable Systems and Data Management D

```sh
# install deps
uv sync

# run runner
uv run cep-run --patterns config/citibike_patterns.yaml --input data/JC-202508-citibike-tripdata.csv --mode citibike

# with timeout
timeout 60 uv run cep-run --patterns config/citibike_patterns.yaml --input data/JC-202508-citibike-tripdata.csv --mode citibike

```
