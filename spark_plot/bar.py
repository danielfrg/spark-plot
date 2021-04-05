import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, LongType, FloatType, DoubleType, DecimalType

from . import utils


def calc(
    df,
    x: str,
    y=None,
    agg=F.count,
    sort_by=None,
    sort_asc=False,
    sort_desc=True,
):
    """
    Calculate a metric (Count) for a discrete column

    Returns a pd.DataFrame

    """

    if y is not None and isinstance(y, str):
        y = [y]

    # Use only a subset of the data we need
    cols = [x]
    if y is not None:
        cols.extend(y)
    data = df[cols]

    # By default aggregate by x, if y is present do it by y
    aliases = []
    aggregates = []
    if y is None:
        # If only x then we always do count(x)
        agg = F.count
        agg_fn = agg(x)
        n_alias = make_alias(agg, x)
        aliases.append(n_alias)
        count_x_agg = agg_fn.alias(n_alias)
        aggregates.append(count_x_agg)
    else:
        for y_col in y:
            agg_fn = agg(y_col)
            n_alias = make_alias(agg, y_col)
            aliases.append(n_alias)
            new_agg = agg_fn.alias(n_alias)
            aggregates.append(new_agg)

    # Calculate
    result = data.groupBy(x).agg(*aggregates)

    if sort_by is not None:
        order_by = F.col(make_alias(agg, sort_by))
        if sort_asc:
            order_by = order_by.asc()
        elif sort_desc:
            order_by = order_by.desc()

        result = result.orderBy(order_by)

    # Make Pandas DF
    pd_df = result.toPandas()
    pd_df = pd_df.set_index(x)
    # if y is None:
    #     pd_df[alias]
    # else:
    #     pd_df[y]

    return pd_df


def make_alias(agg, col_name):
    return f"{agg.__name__}({col_name})"
