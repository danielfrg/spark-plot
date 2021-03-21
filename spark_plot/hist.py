import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.ml.feature import Bucketizer
from pyspark.sql.types import IntegerType, LongType, FloatType, DoubleType, DecimalType

from . import utils


def calc(df, column: str, bins=50, bin_width=None):
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
    data = df[[column]]

    int_types = (IntegerType, LongType, FloatType, DoubleType, DecimalType)
    col_type = data.schema.fields[0].dataType
    if not isinstance(col_type, int_types):
        raise ValueError(
            "hist method requires numerical or datetime columns, nothing to plot."
        )

    # Calculate buckets
    buckets = utils.spark_buckets(data, column, bins=bins, bin_width=bin_width)

    # Calculate counts based on the buckets
    bucketizer = Bucketizer(splits=buckets, inputCol=column, outputCol="bucket")
    buckets_df = bucketizer.transform(data)

    histogram = buckets_df.groupby("bucket").agg(F.count(column).alias("count"))
    histogram = histogram.orderBy("bucket", ascending=True)

    # Create weights (locally)
    hist_pd = histogram.toPandas()

    # Create a new DF with complete buckets and empty counts if needed
    full_buckets = pd.DataFrame(columns=["bucket"])
    full_buckets["bucket"] = np.arange(len(buckets))
    full_buckets = full_buckets.merge(hist_pd, on="bucket", how="left")
    weights = full_buckets["count"]

    return buckets, weights
