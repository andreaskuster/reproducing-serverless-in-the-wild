import matplotlib.pyplot as plt
import numpy as np
from matplotlib import colors
from matplotlib.ticker import PercentFormatter

# fix seed
rng = np.random.default_rng(42)

# simple normal distribution plotting

N_points = 1000
n_bins = 60

# Generate two normal distributions
dist1 = rng.standard_normal(N_points)

plt.hist(dist1, color='black', rwidth=0.7)

# plt.savefig("hist.svg")

plt.show()

