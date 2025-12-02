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
@click.option("--max-anomalies", default=100, type=int, help="Maximum anomalies to send for LLM explanation. Set to -1 for no limit (send all).")
@click.option("--metric", "metrics_to_check", default=["total_count"], multiple=True,
              help="Metric column(s) to check for drops. Can be supplied multiple times.")
def run(metrics, sample, max_files, skip_bad_files, no_llm, max_anomalies, metrics_to_check):
    print("Loading metrics...")
    df = load_metrics(metrics, sample=sample, max_files=max_files, skip_bad_files=skip_bad_files)
    # If loader returned a LazyFrame we are operating in streaming mode.
    if isinstance(df, pl.LazyFrame):
        print("Loaded data lazily (streaming). Processing will be executed lazily.")
    else:
        print(f"Loaded {len(df)} rows.")
    print("Running checks...")
    results = []
    any_anoms = False
    for metric_col in metrics_to_check:
        try:
            drops = detect_drops(df, col=metric_col)
        except Exception as e:
            print(f"Skipping metric {metric_col}: {e}")
            continue

        if drops.is_empty():
            print(f"No anomalies detected for '{metric_col}'.")
            continue

        any_anoms = True
        total_anoms = len(drops)
        print(f"Detected {total_anoms} anomalies for '{metric_col}'. Generating explanations...")

        # Interpret negative `max_anomalies` as unlimited (send all anomalies).
        if max_anomalies is None or max_anomalies < 0:
            limit = total_anoms
        else:
            limit = min(max_anomalies, total_anoms)

        for i, row in enumerate(drops.iter_rows(named=True)):
            if i >= limit:
                break
            change = row.get("change_rate", 0)
            if no_llm:
                comment = "LLM disabled by --no-llm; no explanation generated."
            else:
                comment = explain_anomaly(metric_col, change, "map release data validation")
            results.append({"metric": metric_col, "comment": comment})

        if total_anoms > limit:
            print(f"Skipped explanations for {total_anoms - limit} anomalies for '{metric_col}' (limit={limit}).")
        print("Building report...")
        os.makedirs("reports", exist_ok=True)
        build_report(results, "reports/overseer_report.html")
        print("Report complete.")

if __name__ == "__main__":
    run()
