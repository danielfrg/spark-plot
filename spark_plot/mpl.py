import numpy as np
import matplotlib.pyplot as plt
from pandas.plotting._matplotlib import tools as pd_mpl_tools

from . import hist as hist_


def hist(
    df,
    column,
    bins=50,
    bin_width=None,
    ax=None,
    figsize=None,
    layout=None,
    # sharex: bool = False,
    # sharey: bool = False,
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
        # sharex : bool, default False
        # sharey : bool, default False
        grid : bool, default True
        legend: bool = False,
        kwargs : dict, keyword arguments passed to matplotlib.Axes.hist
    """

    buckets, weights = hist_.calc(df, column, bins=bins, bin_width=bin_width)

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

    ax = _axes[0]
    # We need to remove the last item to pass it to Matplotlib an array with the same length as the weights
    x = np.linspace(buckets[0], buckets[-1], len(weights))
    ax.hist(x, len(x), weights=weights, **kwargs)

    #
    ax.set_title(column)
    ax.grid(grid)
    if legend:
        ax.legend()

    return _axes


def set_defaults():
    plt.style.use("ggplot")

    fig_size = plt.rcParams["figure.figsize"]
    fig_size[0] = 10
    fig_size[1] = 8
    plt.rcParams["figure.figsize"] = fig_size
