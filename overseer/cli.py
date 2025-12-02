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
    # Interpret negative `max_anomalies` as unlimited (send all anomalies).
    if max_anomalies is None or max_anomalies < 0:
        global_limit = None
    else:
        global_limit = int(max_anomalies)

    remaining_quota = None if global_limit is None else global_limit

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

        # iterate rows but respect a global quota across all metrics (remaining_quota).
        for row in drops.iter_rows(named=True):
            if remaining_quota is not None and remaining_quota <= 0:
                break
            change = row.get("change_rate", 0)
            if no_llm:
                comment = "LLM disabled by --no-llm; no explanation generated."
            else:
                comment = explain_anomaly(metric_col, change, "map release data validation")

            # include the full row dict for richer reporting (template may ignore extra keys)
            results.append({"metric": metric_col, "comment": comment, "row": row})

            if remaining_quota is not None:
                remaining_quota -= 1

        skipped = 0
        if remaining_quota is not None and total_anoms > 0:
            shown_for_this = min(total_anoms, global_limit) if global_limit is not None else total_anoms
            # compute skipped based on remaining_quota after processing this metric
            # if remaining_quota went negative/zero, determine how many were skipped for this metric
            processed = min(total_anoms, (global_limit - remaining_quota) if global_limit is not None else total_anoms)
            skipped = total_anoms - min(total_anoms, processed)
        elif global_limit is None:
            skipped = 0

        if skipped > 0:
            print(f"Skipped explanations for {skipped} anomalies for '{metric_col}' (quota exhausted).")

    # After processing all metrics, build the report once with all results collected.
    if not any_anoms:
        print("No anomalies detected for any metric.")
    else:
        print(f"Collected {len(results)} explanations (across metrics). Building report...")
        os.makedirs("reports", exist_ok=True)
        build_report(results, "reports/overseer_report.html")
        print("Report complete.")

if __name__ == "__main__":
    run()
