import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.ml.feature import Bucketizer
from pyspark.sql.types import IntegerType, LongType, FloatType, DoubleType, DecimalType

from . import utils


def calc(df, col_x: str, col_y: str, bins=50, bin_width=None):
    """
    Calculate the buckets and weights for a histogram

    Returns
    -------
        (buckets, weights): tuple of two lists
    """
    # Calculate buckets
    data = df[[col_x, col_y]]

    # Check
    int_types = (IntegerType, LongType, FloatType, DoubleType, DecimalType)
    col_type = data.schema.fields[0].dataType
    if not isinstance(col_type, int_types):
        raise ValueError(
            "hist2d method requires numerical or datetime columns, nothing to plot."
        )

    # Calculate buckets
    buckets_x = utils.spark_buckets(data, col_x, bins=bins, bin_width=bin_width)
    buckets_y = utils.spark_buckets(data, col_y, bins=bins, bin_width=bin_width)

    # Generate DF with buckets
    bucketizer = Bucketizer(splits=buckets_x, inputCol=col_x, outputCol="bucket_x")
    buckets_df = bucketizer.transform(data)
    bucketizer = Bucketizer(splits=buckets_y, inputCol=col_y, outputCol="bucket_y")
    buckets_df = bucketizer.transform(buckets_df)

    histogram = buckets_df.groupby("bucket_x", "bucket_y").agg(
        F.count(col_x).alias("count")
    )

    # Create weights matrix (locally)
    hist_pd = histogram.toPandas()
    weights = np.zeros((bins, bins))
    for index, row in hist_pd.iterrows():
        weights[int(row["bucket_x"]), int(row["bucket_y"])] = row["count"]

    # Mask values that are zero so they look transparent
    weights = np.ma.masked_where(weights == 0, weights)

    len(buckets_x)
    len(weights)

    return buckets_x, buckets_y, weights
