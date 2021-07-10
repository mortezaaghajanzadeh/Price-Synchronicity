#%%
import pandas as pd
import seaborn as sns
from numpy import log as ln
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

path = r"G:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\Price Synchronocity\\"
df = pd.read_csv(path + "priceSynchronocity.csv")
# %%
df["SYNCH"] = ln(df.Rsquared / (1 - df.Rsquared))
df["Grouped"] = 1
df.loc[df.uo.isnull(), "Grouped"] = 0
df = df[df.year < 1399]
df["size"] = ln(df["size"])
df["liquidity"] = ln(df["liquidity"])
#%%
fig = plt.figure(figsize=(8, 4))
g = sns.lineplot(data=df, x="year", y="SYNCH")


pathS = (
    r"D:\Dropbox\Finance(Prof.Heidari-Aghajanzadeh)\Project\Price-Synchronocity\report"
)

plt.ylabel("Synchronicity")
plt.xlabel("Year")
plt.title("Firms' synchronocity Time Series")
fig.set_rasterized(True)
plt.savefig(pathS + "\\SYNCHtimeSeries.eps", rasterized=True, dpi=300)
plt.savefig(pathS + "\\SYNCHtimeSeries.png", bbox_inches="tight")
#
# %%
df["leverage"] = df.Debt / df.BookValue
mlist = [
    "SYNCH",
    "Rsquared",
    "cfr",
    "cr",
    "volatility",
    "liquidity",
    "size",
    "leverage",
    "noind",
]
df[~(df.uo.isnull())][mlist].describe().T
# %%
