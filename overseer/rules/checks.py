import polars as pl


def detect_drops(df, col="total_count", threshold=0.05):
    # Support both eager DataFrame and LazyFrame inputs.
    if isinstance(df, pl.LazyFrame):
        lf = df.with_columns(((pl.col(col).diff()) / pl.col(col).shift(1)).alias("change_rate"))
        res = lf.filter(pl.col("change_rate") < -threshold).collect()
        return res

    # eager DataFrame
    if col not in df.columns:
        raise ValueError(f"Column {col} not found in dataframe.")
    df = df.with_columns((df[col].diff() / df[col].shift(1)).alias("change_rate"))
    return df.filter(df["change_rate"] < -threshold)
