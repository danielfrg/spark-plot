import pyspark.sql.functions as F


def spark_buckets(df, column, bins=30, bin_width=None):
    if bins is None and bin_width is None:
        raise ValueError("Must indicate either bins or bin_width")
    elif bins is None and bin_width is not None:
        raise ValueError("bins and bin_width arguments are mutually exclusive")

    col_df = df[[column]]
    limits = col_df.agg(F.min(column), F.max(column)).collect()
    min_, max_ = limits[0][0], limits[0][1]
    if bin_width is None:
        bin_width = (max_ - min_) / (bins)

    buckets = [min_ + i * bin_width for i in range(bins + 1)]
    return buckets
