import numpy as np
import matplotlib.pyplot as plt


from . import hist as hist_


def hist(df, column, bins=30, binwidth=None, *args, **kwargs):
    buckets, weights = hist_.calc(df, column, bins=bins, binwidth=binwidth)

    # We need to remove the last item to pass it to Matplotlib an array with the same length as the weights
    x = np.linspace(buckets[0], buckets[-1], len(weights))
    _, _, patches = plt.hist(x, len(x), weights=weights, *args, **kwargs)

    return patches


def set_defaults():
    plt.style.use("ggplot")

    fig_size = plt.rcParams["figure.figsize"]
    fig_size[0] = 10
    fig_size[1] = 8
    plt.rcParams["figure.figsize"] = fig_size
