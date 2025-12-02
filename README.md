# Created by
Kartikeya Kumaria

# Overseer
An AI agent for Overture maps. This is my project A implementation prototype

# Must include in root directory
Metrics folder unzipped + python venv. Shouldn't be pushed through Git, hence the .gitignore file

# main command to run from root
 python -m overseer.cli --metrics ./Metrics/metrics

# test command for 1000 files
python -m overseer.cli --metrics ./Metrics/metrics --sample 100000 --skip-bad-files