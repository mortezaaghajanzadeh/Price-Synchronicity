#%%
from numpy.core.arrayprint import _make_options_dict
from numpy.lib.function_base import average
import pandas as pd
import statsmodels.api as sm
# %%
g['Residual'].skew()
# %%
from numpy import log as ln
import re as ree


def convert_ar_characters(input_str):

    mapping = {
        "ك": "ک",
        "گ": "گ",
        "دِ": "د",
        "بِ": "ب",
        "زِ": "ز",
        "ذِ": "ذ",
        "شِ": "ش",
        "سِ": "س",
        "ى": "ی",
        "ي": "ی",
    }
    return _multiple_replace(mapping, input_str)


def _multiple_replace(mapping, text):
    pattern = "|".join(map(ree.escape, mapping.keys()))
    return ree.sub(pattern, lambda m: mapping[m.group()], str(text))

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
df["year"] = df.jalaliDate.apply(year)
df = df.groupby(["group_name", "year"]).filter(lambda x: x.shape[0] >= 2)


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

gg = df.groupby(["name", "yearWeek"])
wdf = gg.last()

wdf = wdf[
        [
            "jalaliDate",
            "date",
            "title",
            "stock_id",
            "group_name",
            "close_price",
            "value",
            "volume",
            "quantity",
            "group_id",
            "industry_index",
        ]
    ]
for i in ["value","volume","quantity"]:
    print(i)
    tempt = gg[i].sum().to_frame()
    mapdict = dict(zip(tempt.index,tempt[i]))
    wdf[i] = wdf.index.map(mapdict)

wdf = wdf.reset_index()
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
wdf["year"] = wdf.jalaliDate.apply(year)
#%%
market = pd.read_excel(path + "IRX6XTPI0009.xls").rename(
    columns={"<DTYYYYMMDD>": "date", "<CLOSE>": "market_index"}
)

market["date"] = market.date.astype(float)
wdf["date"] = wdf.date.astype(float)
mapdict = dict(zip(market["date"], market["market_index"]))
wdf["market_index"] = wdf["date"].map(mapdict)
# %%
shrout = pd.read_csv(path + "SymbolShrout-Annual.csv")
col = "name"
wdf[col] = wdf[col].apply(lambda x: convert_ar_characters(x))
mapdict = dict(zip(shrout.set_index(['symbol','year']).index,shrout.shrout))
wdf['shrout'] = wdf.set_index(['name','year']).index.map(mapdict)

#%%

wdf = wdf[~wdf.shrout.isnull()]
wdf['marketCap'] = wdf.close_price * wdf.shrout
gg = wdf.groupby(['group_name','yearWeek'])

g = gg.get_group(
    (
        'فلزات اساسي',
        '2011-29'
    )
)

def weight_ind(g):
    return g/g.sum()

wdf['weight_ind'] = gg.marketCap.apply(weight_ind)

#%%
wdf["return"] = wdf.groupby("name").close_price.pct_change()
wdf["return_industry"] = wdf.groupby("name").industry_index.pct_change()
wdf["return_market"] = wdf.groupby("name").market_index.pct_change()
wdf["return_industry"]  = (wdf.return_industry - wdf.weight_ind*wdf['return'])/(1-wdf.weight_ind)
wdf["lagReturn"] = wdf.groupby("name")["return"].shift()
wdf["lagReturn_industry"] = wdf.groupby("name")["return_industry"].shift()
wdf["lagReturn_market"] = wdf.groupby("name")["return_market"].shift()

# %%

gg = wdf[(~wdf.market_index.isnull())&
         (~wdf.industry_index.isnull())
         ].groupby(["name", "year"])
# g = gg.get_group((
#     "آكنتور",
#     1387))
#%%
def rCalculation(g):
    # print(g.name[0])
    if len(g)<30:
        return pd.DataFrame()
    y = "return"
    x = ["return_market", "lagReturn_market", "return_industry", "lagReturn_industry"]
    g = g.dropna()
    try:
        # Add a constant term like so:
        model = sm.OLS(g[y], sm.add_constant(g[x])).fit()
        g['Rsquared'] = model.rsquared
        g['Residual'] = model.resid
        return g
    except:
        return pd.DataFrame()


data = gg.apply(rCalculation).reset_index(drop=True)


# %%
data['Firm_Specific_Return'] = ln(1+ data.Residual)
gg = data.groupby(["name", "year"])
g = gg.get_group((
    "شستا",
    1399))
def crashCalculater(g,k):
    std ,average = g.std(),g.mean()
    return g.loc[(g < average - k*std)].size

tempt = gg['Residual'].apply(
    crashCalculater , k = 2
    ).to_frame() 
mapdict = dict(zip(tempt.index,tempt.Residual))
data['NCrash'] = data.set_index(["name", "year"]).index.map(mapdict)
#%%
gg = data.groupby(["name", "year"])
data2 = gg.last()
data2['skew'] = gg.Firm_Specific_Return.skew()
def kurtcal(g):
    return g.kurt()
data2['kurt'] = gg['Firm_Specific_Return'].apply(kurtcal)
data2['return'] = gg['return'].sum()
data2['return_market'] = gg['return_market'].sum()
data2['return_industry_std'] = gg['return_industry'].std()
data2['return_industry'] = gg['return_industry'].sum()
data2 = data2.reset_index()
data = data2[
    [
        'name',
        'year',
        'stock_id',
        'group_name',
        'close_price',
        'value',
        'volume',
        'quantity',
        'group_id',
        'shrout',
        'marketCap',
        'weight_ind',
        'return',
        'return_industry',
        'return_market',
        'Rsquared',
        'Firm_Specific_Return',
        'NCrash',
        'skew',
        'kurt',
        'return_industry_std'
    ]
]
#%%

def BG(df):

    pathBG = r"G:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\Control Right - Cash Flow Right\\"
    # pathBG = r"C:\Users\RA\Desktop\RA_Aghajanzadeh\Data\\"
    n = pathBG + "Grouping_CT.xlsx"
    BG = pd.read_excel(n)


    
    print(len(BG))
    BG =  BG[BG.listed == 1].groupby(["uo", "year"]
                                     ).filter(lambda x: x.shape[0] >= 3)
    print(len(BG))
    BGroup = set(BG["uo"])
    names = sorted(BGroup)
    ids = range(len(names))
    mapingdict = dict(zip(names, ids))
    BG["BGId"] = BG["uo"].map(mapingdict)


    BG = BG.groupby(["uo", "year"]).filter(lambda x: x.shape[0] > 3)
    for i in ["uo", "cfr", "cr", "position", "centrality"]:
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
data = data.rename(columns = {'marketCap':'size'})
#%%
n2 = path + "balance sheet - 9811" + ".xlsx"
df2 = pd.read_excel(n2)
df2 = df2.iloc[:, [0, 4, 13, -9]]
df2.rename(
    columns={
        df2.columns[0]: "symbol",
        df2.columns[1]: "Year",
        df2.columns[2]: "BookValue",
        df2.columns[3]: "Debt",
    },
    inplace=True,
)
def vv5(row):
    X = row.split("/")
    return int(X[0])
df2["Year"] = df2["Year"].apply(vv5)
col = "symbol"
df2[col] = df2[col].apply(lambda x: convert_ar_characters(x))
for i in ['BookValue','Debt'] :
    print(i)
    mapdict = dict(zip(df2.set_index(['symbol','Year']).index,df2[i]))
    data[i] = data.set_index(['name','year']).index.map(mapdict)
#%%
nind = data.groupby(['year','group_id']).size().to_frame()
mapdict = dict(zip(nind.index,nind[0]))
data['noind'] = data.set_index(['year','group_id']).index.map(mapdict)
#%%
mlist = data.name.unique()
mapdict = dict(zip(mlist, range(len(mlist))))
data["id"] = data.name.map(mapdict)
path = r"G:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\Price Synchronocity\\"
data[(data.Rsquared < 1)].to_csv(
    path + "priceSynchronocity.csv", index=False
)
# %%
