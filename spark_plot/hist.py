import pyspark.sql.functions as F
from pyspark.ml.feature import Bucketizer


def calc(df, column, bins=30, binwidth=None):
    """
    Calculate the buckets and weights for a histogram

    Returns
    -------
        (buckets, weights): tuple of two lists
    """

    if bins is None and binwidth is None:
        raise ValueError("Must indicate bins or binwidth")
    elif bins is None and binwidth is not None:
        raise ValueError("bins and binwidth arguments are mutually exclusive")

    # Calculate buckets
    col_df = df[[column]]
    limits = col_df.agg(F.min(column), F.max(column)).collect()
    min_, max_ = limits[0][0], limits[0][1]

    if binwidth is None:
        binwidth = (max_ - min_) / (bins)
    buckets = [min_ + i * binwidth for i in range(bins + 1)]

    # Calculate counts based on the buckets
    bucketizer = Bucketizer(splits=buckets, inputCol=column, outputCol="bucket")
    buckets_df = bucketizer.setHandleInvalid("keep").transform(col_df)

    histogram = buckets_df.groupby("bucket").agg(F.count(column).alias("count"))
    histogram = histogram.orderBy("bucket", ascending=True)

    # Return list of values
    col_rows = histogram.select("count").collect()
    weights = [row[0] for row in col_rows]

    return buckets, weights
