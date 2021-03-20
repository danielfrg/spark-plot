import pyspark.sql.functions as F
from pyspark.ml.feature import Bucketizer
from pyspark.sql.types import IntegerType, LongType, FloatType, DoubleType, DecimalType


def calc(df, column: str, bins=30, bin_width=None):
    """
    Calculate the buckets and weights for a histogram

    Returns
    -------
        (buckets, weights): tuple of two lists
    """
    if bins is None and bin_width is None:
        raise ValueError("Must indicate bins or bin_width")
    elif bins is None and bin_width is not None:
        raise ValueError("bins and bin_width arguments are mutually exclusive")

    # Calculate buckets
    col_df = df[[column]]

    int_types = (IntegerType, LongType, FloatType, DoubleType, DecimalType)
    col_type = col_df.schema.fields[0].dataType
    if not isinstance(col_type, int_types):
        raise ValueError(
            "hist method requires numerical or datetime columns, nothing to plot."
        )

    limits = col_df.agg(F.min(column), F.max(column)).collect()
    min_, max_ = limits[0][0], limits[0][1]

    if bin_width is None:
        bin_width = (max_ - min_) / (bins)
    buckets = [min_ + i * bin_width for i in range(bins + 1)]

    # Calculate counts based on the buckets
    bucketizer = Bucketizer(splits=buckets, inputCol=column, outputCol="bucket")
    buckets_df = bucketizer.setHandleInvalid("keep").transform(col_df)

    histogram = buckets_df.groupby("bucket").agg(F.count(column).alias("count"))
    histogram = histogram.orderBy("bucket", ascending=True)

    # Return list of values
    col_rows = histogram.select("count").collect()
    weights = [row[0] for row in col_rows]

    return buckets, weights
