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

# important note about Open AI api key
Make sure to set the Open AI key using Powershell or Terminal locally before running the main command

## Overseer CLI Commands

This project exposes a single CLI entrypoint implemented in `overseer/cli.py`. The CLI loads metric CSVs from a folder, runs a set of checks, optionally uses an LLM to explain detected anomalies, and writes an HTML report. Run it from the project root with `python -m overseer.cli`.

Each option is described below with details and usage notes.

- `--metrics` (string, default: `Metrics/metrics`)
  - Description: Path to the directory that contains metric CSV files to process. The loader expects a collection of CSV files in that folder (the project includes a `Metrics/metrics` layout by default).
  - Example: `--metrics ./Metrics/metrics` or a full path like `--metrics C:/data/metrics`.

- `--sample` (int, default: none)
  - Description: Limit the total number of rows read across all files. This is intended for safe testing on a small subset of the dataset and prevents long/full runs during development.
  - Behavior: When supplied, the loader will stop after reading approximately the requested number of rows (best-effort). Use this to verify pipelines quickly without consuming large amounts of memory/CPU.
  - Example: `--sample 100000` (read about 100k rows total).

- `--max-files` (int, default: none)
  - Description: Limit the number of files processed from the metrics folder. Useful when you want to test only the first N files without editing the dataset.
  - Example: `--max-files 10` (process only the first 10 metric files found).

- `--skip-bad-files` (flag, default: off)
  - Description: When present, the loader will skip files that fail to parse (for example, badly-formed CSVs) and continue processing the rest. Without this flag, a parse error on any file will typically abort the whole run.
  - Use case: Helpful when the metrics folder contains occasional malformed files and you prefer a partial result rather than a full failure.

- `--no-llm` (flag, default: off)
  - Description: Disable all LLM (OpenAI) calls. When set, the CLI will not attempt to contact any language model for generating explanations and will instead insert placeholder messages for each anomaly.
  - Use case: Run in environments without an OpenAI key, to avoid API costs, or for deterministic, offline runs.

- `--max-anomalies` (int, default: 100)
  - Description: Limits how many detected anomalies will be sent to the LLM for explanation. This caps potential cost and run-time when a large number of anomalies are found.
  - Behavior: The CLI detects all anomalies but will only request explanations for up to `--max-anomalies`. Remaining anomalies (if any) are left unexplained in the report.

### What the CLI does (behavior details)

- Loading: The CLI calls the project's loader which will either read files eagerly or operate in streaming (lazy) mode for large datasets. When streaming is used you will see the message: `Loaded data lazily (streaming). Processing will be executed lazily.`

- Anomaly detection: After load, the CLI runs a set of checks (for example, `detect_drops`) against the loaded dataset. The checks produce a small table of detected anomalies with columns such as `change_rate` or similar metric-specific fields.

- Explanations: For each anomaly (up to `--max-anomalies`) the CLI will ask the LLM to produce a short human-readable explanation unless `--no-llm` is set. The LLM call is wrapped in safe fallbacks so a missing API key or HTTP error does not crash the whole run.

- Reporting: The CLI writes an HTML report to `reports/overseer_report.html`. The `reports` directory is created automatically if missing.

### Output and artifacts

- `reports/overseer_report.html`: the generated HTML report containing detected anomalies and explanations (LLM-generated or placeholder).
- `.stream_parquet/` (optional): when the loader runs in streaming mode it may write temporary per-file Parquet tiles into a `.stream_parquet` cache inside the metrics folder. This speeds repeated runs and avoids re-parsing CSVs; you can safely remove this folder after a successful run to reclaim disk space.

### Examples (PowerShell)

Run a quick sample without LLM explanations and skip files that fail parsing:
```powershell
python -m overseer.cli --metrics ./Metrics/metrics --sample 100000 --skip-bad-files --no-llm