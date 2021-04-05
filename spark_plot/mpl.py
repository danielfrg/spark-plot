import numpy as np
import matplotlib.pyplot as plt
from pandas.plotting._matplotlib import tools as pd_mpl_tools
import pyspark.sql.functions as F

from . import bar as bar_
from . import hist as hist_
from . import hist2d as hist2d_


def bar(
    df,
    x,
    y=None,
    agg=F.count,
    # alias=None,
    sort_by=None,
    sort_asc=False,
    sort_desc=True,
    **kwargs,
):
    pandas_df = bar_.calc(
        df,
        x=x,
        y=y,
        agg=agg,
        # alias=alias,
        sort_by=sort_by,
        sort_asc=sort_asc,
        sort_desc=sort_desc,
    )

    ax = pandas_df.plot.bar(**kwargs)
    return ax


def hist(
    df,
    column,
    bins: int = 50,
    bin_width=None,
    ax=None,
    figsize=None,
    layout=None,
    grid: bool = True,
    legend: bool = False,
    **kwargs,
):
    """
    Parameters
    ----------
        df: Spark DataFrame
        column: string
        bins : int, default 50
        bin_width: int, default None. Used if bins=None
        ax : axes, optional
        figsize : tuple, optional
        layout : optional
        grid : bool, default True
        legend: bool = False,
        kwargs : dict, keyword arguments passed to matplotlib.Axes.hist
    """

    def hist_func(ax):
        # Calculate buckets
        buckets, weights = hist_.calc(df, column, bins=bins, bin_width=bin_width)
        # We need to remove the last item to pass it to Matplotlib an array with the same length as the weights
        return ax.hist(buckets, len(buckets), weights=weights, **kwargs)

    return plot(
        hist_func,
        ax=ax,
        figsize=figsize,
        layout=layout,
        title=column,
        grid=grid,
        legend=legend,
    )


def hist2d(
    df,
    col_x,
    col_y,
    bins: int = 100,
    bin_width=None,
    ax=None,
    figsize=None,
    layout=None,
    title=None,
    grid: bool = True,
    legend: bool = False,
    colorbar: bool = True,
    **kwargs,
):
    """
    Parameters
    ----------
        df: Spark DataFrame
        column: string
        bins : int, default 50
        bin_width: int, default None. Used if bins=None
        ax : axes, optional
        figsize : tuple, optional
        layout : optional
        grid : bool, default True
        legend: bool = False,
        colorbar: bool = True,
        kwargs : dict, keyword arguments passed to matplotlib.Axes.hist
    """

    def hist2d_func(ax):
        buckets_x, buckets_y, weights = hist2d_.calc(
            df, col_x, col_y, bins=bins, bin_width=bin_width
        )

        return ax.pcolormesh(buckets_x, buckets_y, weights.T, **kwargs)

    return plot(
        hist2d_func,
        ax=ax,
        figsize=figsize,
        layout=layout,
        title=title,
        xlabel=col_x,
        ylabel=col_y,
        grid=grid,
        legend=legend,
        colorbar=colorbar,
    )


def plot(
    plot_func,
    ax=None,
    figsize=None,
    layout=None,
    title=None,
    xlabel=None,
    ylabel=None,
    grid: bool = True,
    legend: bool = False,
    colorbar: bool = False,
):
    # TODO: dynamic axes with column = None as default
    naxes = 1
    fig, axes = pd_mpl_tools.create_subplots(
        naxes=naxes,
        ax=ax,
        figsize=figsize,
        layout=layout,
        # sharex=sharex,
        # sharey=sharey,
        squeeze=False,
    )
    _axes = pd_mpl_tools.flatten_axes(axes)

    # Make the target plot
    ax = _axes[0]
    p = plot_func(ax)

    # Other MPL things
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(grid)
    if legend:
        ax.legend()
    if colorbar:
        fig.colorbar(p, ax=ax)

    return _axes


def set_defaults():
    plt.style.use("ggplot")
    set_fig_size(10, 8)


def set_fig_size(x, y):
    fig_size = plt.rcParams["figure.figsize"]
    fig_size[0] = x
    fig_size[1] = y
    plt.rcParams["figure.figsize"] = fig_size
