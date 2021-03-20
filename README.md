# spark-plot

[![PyPI](https://badge.fury.io/py/spark-plot.svg)](https://pypi.org/project/spark-plot/)
[![Testing](https://github.com/danielfrg/spark-plot/workflows/test/badge.svg)](https://github.com/danielfrg/spark-plot/actions)
[![License](https://img.shields.io/:license-Apache%202-blue.svg)](https://github.com/danielfrg/spark-plot/blob/master/LICENSE.txt)

Simplifies plotting Spark DataFrames by make calculations for plots in Spark.
Supports (soon) multiple Python plotting frontends

## Installation

```
pip install spark-plot
```

## Usage

A simple example using Matplotlib

```python
## Create an Spark DataFrame
from nycflights13 import flights as flights_pd

flights = spark.createDataFrame(flights_pd)

# Make a Histogram

from spark_plot import mpl

fig = plt.figure()

# spark-plot code
mpl.hist(flights, "distance", color="#474747")

plt.title("Distance")
plt.show()
```

![Flights Histogram](https://github.com/danielfrg/spark-plot/raw/main/docs/flights_hist.png "Flights Distance Histogram")
