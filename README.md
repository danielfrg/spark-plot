# spark-plot

[![pypi](https://badge.fury.io/py/spark-plot.svg)](https://pypi.org/project/spark-plot/)
[![build](https://github.com/danielfrg/spark-plot/workflows/test/badge.svg)](https://github.com/danielfrg/spark-plot/actions/workflows/test.yml)
[![license](https://img.shields.io/:license-Apache%202-blue.svg)](https://github.com/danielfrg/spark-plot/blob/main/LICENSE.txt)

- Simplifies plotting Spark DataFrames by making calculations for plots inside Spark
- Plot types: Histogram, 2D Histogram
- Generates Matplotlib plots with a similar [Pandas Plotting API](https://pandas.pydata.org/docs/user_guide/visualization.html)

TODO:
- Other plot types
- Supports multiple Python plotting frontends (Altair, Plotly and more)

## Installation

```
pip install spark-plot
```

## Usage

**Look at the full** [NYCFlights example notebook](https://nbviewer.extrapolations.dev/nb/raw.githubusercontent.com/danielfrg/spark-plot/main/notebooks/nycflights.ipynb). A short summary is presented below.

Create an Spark DataFrame:

```python
from nycflights13 import flights as flights_pd

flights = spark.createDataFrame(flights_pd)
```

Import `spark-plot` [Matplotlib](https://matplotlib.org/stable/index.html) frontend:

```python
from spark_plot import mpl
```

### Histogram

```python
mpl.hist(flights, "distance", color="#474747")
```

![Flights Histogram](https://github.com/danielfrg/spark-plot/raw/main/docs/flights_hist.png "Flights Distance Histogram")

Specify the `bin_width` instead:

```python
mpl.hist(flights, "distance", bin_width=400, color="#474747")
```

![Flights Histogram](https://github.com/danielfrg/spark-plot/raw/main/docs/flights_hist_bin_width.png "Flights Distance Histogram")

### Histogram 2D

Similar to a histogram but in two dimensions.

```python
ax = mpl.hist2d(flights, col_x="sched_dep_time", col_y="sched_arr_time", title="Sched Arrival vs Departure", cmap="Blues_r")
```

![Flights Histogram 2d](https://github.com/danielfrg/spark-plot/raw/main/docs/flights_hist2d.png "Flights Scheduled 2D Histogram")

### Bar plot

```python
mpl.bar(flights, x="origin")
```

![Flights Bar Plot](https://github.com/danielfrg/spark-plot/raw/main/docs/flights_bar_origin.png)

#### Aggregate Functions

Use any Spark aggregate function

```python
import pyspark.sql.functions as F

mpl.bar(flights, x="origin", y="dep_delay", agg=F.mean)
```

![Flights Bar Plot Mean Delay](https://github.com/danielfrg/spark-plot/raw/main/docs/flights_bar_origin_depdelay.png)

#### Multiple columns

Pass multiple columns.

```python
import pyspark.sql.functions as F

ax = mpl.bar(flights, x="origin", y=["dep_delay", "arr_delay"], agg=F.sum)
```

![Flights Bar Plot Sum delays](https://github.com/danielfrg/spark-plot/raw/main/docs/flights_bar_origin_sum_delays.png)
