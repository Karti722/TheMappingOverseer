import sys
from pathlib import Path

import polars as pl
from inspect import signature
import tempfile
import shutil
import csv as _csv
import io

try:
    import pandas as pd  # optional dependency used only for a tolerant fallback
except Exception:  # pragma: no cover - handled at runtime if pandas not installed
    pd = None


def _read_csv_with_fallback(path: Path) -> pl.DataFrame:
    try:
        return pl.read_csv(path)
    except Exception:
        # First fallback: attempt common CSV variations (backslash-escaped quotes,
        # larger schema inference). Some polars versions don't accept all kwargs
        # (eg. `escape_char`) so filter kwargs to the read_csv signature.
        try:
            csv_kwargs = dict(
                ignore_errors=True,
                quote_char='"',
                escape_char='\\',
                infer_schema_length=10000,
            )
            sig = signature(pl.read_csv)
            filtered_kwargs = {k: v for k, v in csv_kwargs.items() if k in sig.parameters}
            return pl.read_csv(path, **filtered_kwargs)
        except Exception as e:
            # If polars couldn't read even with filtered kwargs, try a tolerant
            # pandas reader (engine='python') which can handle malformed quoting
            # and then convert to a polars DataFrame. If pandas isn't available
            # or also fails, raise a clear RuntimeError.
            if pd is None:
                raise RuntimeError(
                    f"Failed to read CSV {path}: {e} (pandas not installed for fallback)"
                ) from e

            try:
                # Read everything as string to avoid dtype inference problems.
                # Use the python engine and be tolerant of bad lines / quoting.
                df_pd = pd.read_csv(
                    path,
                    dtype=str,
                    engine="python",
                    sep=",",
                    escapechar="\\",
                    quoting=_csv.QUOTE_NONE,
                    on_bad_lines="skip",
                )
                # Convert the cleaned pandas CSV to bytes and let polars
                # parse that CSV. This avoids needing pyarrow for
                # pandas->polars conversion.
                csv_bytes = df_pd.to_csv(index=False).encode("utf-8")
                return pl.read_csv(io.BytesIO(csv_bytes))
            except Exception as e2:
                raise RuntimeError(f"Failed to read CSV {path}: {e2}") from e2


def load_metrics(metrics_dir: str, sample: int = None, max_files: int = None, skip_bad_files: bool = False):
    """
    Stream CSV files by reading them one-by-one, writing per-file Parquet to a
    temporary directory, and returning a LazyFrame that scans those Parquet
    files. This avoids materializing the entire dataset in memory.

    - `sample`: optional int - stop after writing approx this many rows.
    - `max_files`: optional int - process at most this many files.
    - `skip_bad_files`: if True, continue past files that fail to parse.
    """

    csvs = list(Path(metrics_dir).rglob("*.csv"))
    if not csvs:
        raise FileNotFoundError(f"No CSV files found in {metrics_dir}")

    tmpdir = Path(metrics_dir) / ".stream_parquet"
    # Fresh tmp dir
    if tmpdir.exists():
        shutil.rmtree(tmpdir)
    tmpdir.mkdir(parents=True, exist_ok=True)

    # First pass: determine canonical column set by reading headers only.
    all_cols = []
    seen = set()
    for i, f in enumerate(csvs):
        if max_files is not None and i >= max_files:
            break
        try:
            hdr = pl.read_csv(f, n_rows=0)
            cols = hdr.columns
        except Exception:
            # fallback to pandas for robust header parsing
            if pd is None:
                if skip_bad_files:
                    continue
                raise RuntimeError(f"Failed to read header for CSV {f}")
            try:
                cols = list(pd.read_csv(f, nrows=0, engine="python").columns)
            except Exception:
                if skip_bad_files:
                    continue
                raise RuntimeError(f"Failed to read header for CSV {f}")

        for c in cols:
            if c not in seen:
                seen.add(c)
                all_cols.append(c)

    # Ensure canonical columns has at least the key metric
    numeric_cols = {"total_count"}
    for nc in numeric_cols:
        if nc not in seen:
            seen.add(nc)
            all_cols.append(nc)

    # Infer canonical types by sampling up to a few files and rows per file.
    canonical_types = {}
    sample_files = csvs[: min(len(csvs), 5) ]
    sample_rows = 200
    numeric_re = r'^[+-]?\d+(?:\.\d+)?$'
    for col in all_cols:
        numeric_votes = 0
        non_null_votes = 0
        for f in sample_files:
            try:
                # use pandas for robust sampling when available
                if pd is not None:
                    df_s = pd.read_csv(f, nrows=sample_rows, dtype=str, engine="python", on_bad_lines="skip")
                    if col not in df_s.columns:
                        continue
                    ser = df_s[col].dropna().astype(str)
                    if len(ser) == 0:
                        continue
                    non_null_votes += len(ser)
                    numeric_votes += ser.str.match(numeric_re).sum()
                else:
                    # fallback to polars sampling
                    df_s = pl.read_csv(f, n_rows=sample_rows)
                    if col not in df_s.columns:
                        continue
                    ser = df_s.select(pl.col(col).cast(pl.Utf8)).to_series()
                    ser = ser.drop_nulls()
                    if len(ser) == 0:
                        continue
                    non_null_votes += len(ser)
                    # use python regex on the numpy array
                    import re
                    numeric_votes += sum(1 for v in ser.to_list() if re.match(numeric_re, str(v)))
            except Exception:
                continue
        if non_null_votes == 0:
            canonical_types[col] = pl.Utf8
        else:
            if numeric_votes / non_null_votes >= 0.95:
                canonical_types[col] = pl.Float64
            else:
                canonical_types[col] = pl.Utf8

    total_rows = 0
    files_written = 0
    for f in csvs:
        if max_files is not None and files_written >= max_files:
            break
        try:
            df = _read_csv_with_fallback(f)
        except RuntimeError as e:
            msg = f"Warning: could not read {f}. Error: {e}"
            print(msg, file=sys.stderr)
            if skip_bad_files:
                continue
            raise
        # If sampling by rows is requested, truncate the last file to fit
        if sample is not None:
            remaining = sample - total_rows
            if remaining <= 0:
                break
            if df.height > remaining:
                df = df.head(remaining)

        # Add missing canonical columns (with typed nulls) and cast existing columns
        missing = [c for c in all_cols if c not in df.columns]
        add_cols = []
        for c in missing:
            add_cols.append(pl.lit(None).cast(canonical_types.get(c, pl.Utf8)).alias(c))
        if add_cols:
            df = df.with_columns(add_cols)

        # Cast existing columns to canonical types where possible
        cast_cols = []
        for c in all_cols:
            if c in df.columns and c in canonical_types:
                try:
                    cast_cols.append(pl.col(c).cast(canonical_types[c]).alias(c))
                except Exception:
                    # ignore cast failures per-column
                    pass
        if cast_cols:
            df = df.with_columns(cast_cols)

        # Reorder to canonical column order
        df = df.select([pl.col(c) for c in all_cols])

        # write to parquet file in tmpdir
        out_path = tmpdir / f"part-{files_written:06d}.parquet"
        df.write_parquet(out_path)

        total_rows += df.height
        files_written += 1

        # Stop early if we've hit the sample limit
        if sample is not None and total_rows >= sample:
            break

    if files_written == 0:
        return pl.DataFrame()

    # Return a LazyFrame scanning the parquet files so downstream processing
    # (e.g. detect_drops) can be executed lazily and will only materialize
    # a small result set.
    pattern = str(tmpdir / "*.parquet")
    lf = pl.scan_parquet(pattern)
    return lf
