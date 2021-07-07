#%%
from numpy.core.arrayprint import _make_options_dict
import pandas as pd
import statsmodels.api as sm


def addDash(row):
    row = str(row)
    X = [1, 1, 1]
    X[0] = row[0:4]
    X[1] = row[4:6]
    X[2] = row[6:8]
    return X[0] + "-" + X[1] + "-" + X[2]


def removeSlash(row):
    X = row.split("/")
    if len(X[1]) < 2:
        X[1] = "0" + X[1]
    if len(X[2]) < 2:
        X[2] = "0" + X[2]
    return int(X[0] + X[1] + X[2])


def year(row):
    X = row.split("-")
    return int(X[0])


#%%
path = r"G:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\\"

df = pd.read_parquet(path + "Stocks_Prices_1400-02-07.parquet")
df["date1"] = df["date"].apply(addDash)
df["date1"] = pd.to_datetime(df["date1"])
df["week_of_year"] = df["date1"].dt.week
df["Month_of_year"] = df["date1"].dt.month
df["year_of_year"] = df["date1"].dt.year
df["year_of_year"] = df.year_of_year.astype(str)
df["week_of_year"] = df.week_of_year.astype(str)
df["yearWeek"] = df.year_of_year + "-" + df.week_of_year
df = df[~((df.title.str.startswith("ح")) & (df.name.str.endswith("ح")))]
df = df[~(df.name.str.endswith("پذيره"))]
df = df[~(df.group_name == "زراعت و خدمات وابسته")]
df = df[~(df.group_name == "صندوق سرمايه گذاري قابل معامله")]
	
#%%
gdf = pd.read_parquet(path + "Stocks_Prices_1399-09-12.parquet")[
    ["group_id", "group_name"]
].drop_duplicates()
mapdict = dict(zip(gdf.group_name, gdf.group_id))
df["group_id"] = df.group_name.map(mapdict)
industry = pd.read_csv(path + "indexes_1400-04-09.csv")
industry["date"] = industry.date.apply(removeSlash)
industry["date"] = industry.date.apply(addDash)

mlist = ["overall_index", "EWI"]
industry = industry[~industry.index_id.isin(mlist)]
industry["index_id"] = industry["index_id"].astype(float)
industry = industry.set_index(["index_id", "date"])
mapdict = dict(zip(industry.index, industry["index"]))
df["industry_index"] = df.set_index(["group_id", "jalaliDate"]).index.map(mapdict)
df
#%%
df["close_price"] = df["close_price"].astype(float)
df["value"] = df["value"].astype(float)
df["volume"] = df["volume"].astype(float)
df["quantity"] = df["quantity"].astype(float)
df["industry_index"] = df["industry_index"].astype(float)

wdf = (
    df.groupby(["name", "yearWeek"])
    .last()
    .reset_index()[
        [
            "jalaliDate",
            "date",
            "name",
            "title",
            "stock_id",
            "group_name",
            "close_price",
            "value",
            "volume",
            "quantity",
            "group_id",
            "yearWeek",
            "industry_index",
        ]
    ]
)
wdf = wdf.drop(wdf[(wdf["name"] == "وقوام") & (wdf["close_price"] == 1000)].index)
symbols = [
    "سپرده",
    "هما",
    "وهنر-پذیره",
    "نکالا",
    "تکالا",
    "اکالا",
    "توسعه گردشگری ",
    "وآفر",
    "ودانا",
    "نشار",
    "نبورس",
    "چبسپا",
    "بدکو",
    "چکارم",
    "تراک",
    "کباده",
    "فبستم",
    "تولیددارو",
    "قیستو",
    "خلیبل",
    "پشاهن",
    "قاروم",
    "هوایی سامان",
    "کورز",
    "شلیا",
    "دتهران",
    "نگین",
    "کایتا",
    "غیوان",
    "تفیرو",
    "سپرمی",
    "بتک",
    "آبفا اصفهان",
]
wdf = wdf.drop(wdf[wdf["name"].isin(symbols)].index)
wdf = wdf.drop(
    wdf[
        (
            (wdf.close_price == 10000.0)
            | (wdf.close_price == 1000.0)
            | (wdf.close_price == 5000.0)
            | (wdf.close_price == 100.0)
            | (wdf.close_price == 10.0)
        )
        & (wdf.volume < 1)
    ].index
)
#%%
market = pd.read_excel(path + "IRX6XTPI0009.xls").rename(
    columns={"<DTYYYYMMDD>": "date", "<CLOSE>": "market_index"}
)

market["date"] = market.date.astype(float)
wdf["date"] = wdf.date.astype(float)
mapdict = dict(zip(market["date"], market["market_index"]))
wdf["market_index"] = wdf["date"].map(mapdict)
# %%
wdf["return"] = wdf.groupby("name").close_price.pct_change()
wdf["return_industry"] = wdf.groupby("name").industry_index.pct_change()
wdf["return_market"] = wdf.groupby("name").market_index.pct_change()
wdf["lagReturn"] = wdf.groupby("name")["return"].shift()
wdf["lagReturn_industry"] = wdf.groupby("name")["return_industry"].shift()
wdf["lagReturn_market"] = wdf.groupby("name")["return_market"].shift()
wdf["year"] = wdf.jalaliDate.apply(year)
# %%

gg = wdf[~wdf.return_industry.isnull()].groupby(["name", "year"])

#%%
def rCalculation(g):
    # print(g.name[0])
    y = "return"
    x = ["return_market", "lagReturn_market", "return_industry", "lagReturn_industry"]
    g = g.dropna()
    try:
        # Add a constant term like so:
        model = sm.OLS(g[y], sm.add_constant(g[x])).fit()
        return model.rsquared
    except:
        return


data = gg.apply(rCalculation).to_frame().reset_index().rename(columns={0: "Rsquared"})


# %%


def BG(df):

    pathBG = r"G:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\Control Right - Cash Flow Right\\"
    # pathBG = r"C:\Users\RA\Desktop\RA_Aghajanzadeh\Data\\"
    n = pathBG + "Grouping_CT.xlsx"
    BG = pd.read_excel(n)
    uolist = (
        BG[BG.listed == 1]
        .groupby(["uo", "year"])
        .filter(lambda x: x.shape[0] >= 3)
        .uo.unique()
    )
    print(len(BG))
    BG = BG[BG.uo.isin(uolist)]
    print(len(BG))
    BGroup = set(BG["uo"])
    names = sorted(BGroup)
    ids = range(len(names))
    mapingdict = dict(zip(names, ids))
    BG["BGId"] = BG["uo"].map(mapingdict)

    tt = BG[BG.year == 1397]
    tt["year"] = 1398

    BG = BG.groupby(["uo", "year"]).filter(lambda x: x.shape[0] > 3)
    for i in ["uo", "cfr", "cr"]:
        print(i)
        fkey = zip(list(BG.symbol), list(BG.year))
        mapingdict = dict(zip(fkey, BG[i]))
        df[i] = df.set_index(["name", "year"]).index.map(mapingdict)
    return df


data = BG(data)
# data = data[~data.uo.isnull()]
#%%
df["year"] = df.jalaliDate.apply(year)
df["close_price"] = df.close_price.astype(float)
df = df.sort_values(by=["name", "date"])
df["return"] = df.groupby("name").close_price.pct_change()
df.drop(df[(["name"] == "وقوام") & (df["close_price"] == 1000)].index)
symbols = [
    "سپرده",
    "هما",
    "وهنر-پذیره",
    "نکالا",
    "تکالا",
    "اکالا",
    "توسعه گردشگری ",
    "وآفر",
    "ودانا",
    "نشار",
    "نبورس",
    "چبسپا",
    "بدکو",
    "چکارم",
    "تراک",
    "کباده",
    "فبستم",
    "تولیددارو",
    "قیستو",
    "خلیبل",
    "پشاهن",
    "قاروم",
    "هوایی سامان",
    "کورز",
    "شلیا",
    "دتهران",
    "نگین",
    "کایتا",
    "غیوان",
    "تفیرو",
    "سپرمی",
    "بتک",
    "آبفا اصفهان",
]
df = df.drop(df[df["name"].isin(symbols)].index)
df = df.drop(
    df[
        (
            (df.close_price == 10000.0)
            | (df.close_price == 1000.0)
            | (df.close_price == 5000.0)
            | (df.close_price == 100.0)
            | (df.close_price == 10.0)
        )
        & (df.volume < 1)
    ].index
)
#%%
df["Amihud"] = abs(df["return"]) / df.value
df = df[(df.Amihud < 1e10) & (df.Amihud > -1e10)]
gg = df.groupby(["name", "year"])
g = gg.get_group(("شستا", 1399))
df = df.set_index(["name", "year"])
df["volatility"] = gg["return"].std()
df["liquidity"] = gg["Amihud"].mean()
df = df.reset_index()
#%%
fkey = zip(df.name, df.year)
mapdict = dict(zip(fkey, df.volatility))
data["volatility"] = data.set_index(["name", "year"]).index.map(mapdict)
fkey = zip(df.name, df.year)
mapdict = dict(zip(fkey, df.liquidity))
data["liquidity"] = data.set_index(["name", "year"]).index.map(mapdict)
#%%
groupname = df[["name", "group_id"]].drop_duplicates()
mapdict = dict(zip(groupname.name, groupname.group_id))
data["group_id"] = data.name.map(mapdict)
#%%
sdf = pd.read_csv(path + "SymbolShrout.csv")
sdf['year'] = round(sdf.jalaliDate/10000)
sdf['year'] = sdf['year'].astype(int)
sdf = sdf.set_index(["year", "symbol"])
mapdict = dict(zip(sdf.index, sdf.shrout))
pdf = df[["name", "close_price", "year"]].drop_duplicates(
    subset=["name", "year"]
)
pdf["year"] = pdf.year.astype(int)
pdf["shrout"] = pdf.set_index(["year", "name"]).index.map(mapdict)
pdf["size"] = pdf.close_price * pdf.shrout
pdf
#%%
fkey = zip(pdf.name, pdf.year)
mapdict = dict(zip(fkey, pdf["size"]))
data["size"] = data.set_index(["name", "year"]).index.map(mapdict)
#%%
mlist = data.name.unique()
mapdict = dict(zip(mlist, range(len(mlist))))
data["id"] = data.name.map(mapdict)
path = r"G:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\Price Synchronocity\\"
data[(data.Rsquared >= -1) & (data.Rsquared < 1)].to_csv(
    path + "priceSynchronocity.csv", index=False
)
# %%
