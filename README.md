# spark-plot

[![PyPI](https://badge.fury.io/py/spark-plot.svg)](https://pypi.org/project/spark-plot/)
[![Testing](https://github.com/danielfrg/spark-plot/workflows/test/badge.svg)](https://github.com/danielfrg/spark-plot/actions)
[![License](https://img.shields.io/:license-Apache%202-blue.svg)](https://github.com/danielfrg/spark-plot/blob/master/LICENSE.txt)

- Simplifies plotting Spark DataFrames by making calculations for plots inside Spark
- Plot types: Histogram
- Generates Matplotlib plots with a similar [Pandas Plotting API](https://pandas.pydata.org/docs/user_guide/visualization.html)

TODO:
- Raster plots (2D Histograms)
- Supports multiple Python plotting frontends (Altair, Plotly and more)

## Installation

```
pip install spark-plot
```

## Usage

A simple example using Matplotlib

```python
# Create an Spark DataFrame
from nycflights13 import flights as flights_pd

flights = spark.createDataFrame(flights_pd)

# Make a Histogram

from spark_plot import mpl

mpl.hist(flights, "distance", color="#474747")
```

![Flights Histogram](https://github.com/danielfrg/spark-plot/raw/main/docs/flights_hist.png "Flights Distance Histogram")
