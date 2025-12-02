import click
import polars as pl
from overseer.io.loader import load_metrics
from overseer.rules.checks import detect_drops
from overseer.llm.explainer import explain_anomaly
from overseer.reporting.report_builder import build_report
import os

@click.command()
@click.option("--metrics", default="Metrics/metrics", help="Path to metrics folder.")
@click.option("--sample", default=None, type=int, help="Limit total rows to read (safe testing).")
@click.option("--max-files", default=None, type=int, help="Limit number of files to process.")
@click.option("--skip-bad-files", is_flag=True, help="Skip files that fail to parse instead of aborting.")
@click.option("--no-llm", is_flag=True, help="Do not call the LLM for explanations (safe when no API key).")
@click.option("--max-anomalies", default=100, type=int, help="Maximum anomalies to send for LLM explanation.")
def run(metrics, sample, max_files, skip_bad_files, no_llm, max_anomalies):
    print("Loading metrics...")
    df = load_metrics(metrics, sample=sample, max_files=max_files, skip_bad_files=skip_bad_files)
    # If loader returned a LazyFrame we are operating in streaming mode.
    if isinstance(df, pl.LazyFrame):
        print("Loaded data lazily (streaming). Processing will be executed lazily.")
    else:
        print(f"Loaded {len(df)} rows.")
    print("Running checks...")
    drops = detect_drops(df)
    results = []
    if drops.is_empty():
        print("No anomalies detected.")
    else:
        total_anoms = len(drops)
        print(f"Detected {total_anoms} anomalies. Generating explanations...")
        limit = min(max_anomalies, total_anoms) if max_anomalies is not None else total_anoms
        for i, row in enumerate(drops.iter_rows(named=True)):
            if i >= limit:
                break
            change = row.get("change_rate", 0)
            if no_llm:
                comment = "LLM disabled by --no-llm; no explanation generated."
            else:
                comment = explain_anomaly("total_count", change, "map release data validation")
            results.append({"metric": "total_count", "comment": comment})
        if total_anoms > limit:
            print(f"Skipped explanations for {total_anoms - limit} anomalies (limit={limit}).")
        print("Building report...")
        os.makedirs("reports", exist_ok=True)
        build_report(results, "reports/overseer_report.html")
        print("Report complete.")

if __name__ == "__main__":
    run()
