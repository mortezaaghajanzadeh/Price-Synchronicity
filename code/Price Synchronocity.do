cls
clear
import delimited "G:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\Price Synchronocity\priceSynchronocity.csv", encoding(UTF-8) 


cd "D:\Dropbox\Project\Price Synchronocity\report"

gen SYNCH = log(rsquared / (1-rsquared))

label variable SYNCH "SYNCH"

replace size = log(size)
label variable size "Size"
replace liquidity = log(liquidity)
label variable liquidity "Liquidity"

gen Excess = (cr - cfr)/cr
gen ExcessDiff = cr - cfr

gen ExcessDummy = 0
replace ExcessDummy = 1 if ExcessDiff>0

egen med = median(Excess)

gen ExcessHigh = 0 
replace ExcessHigh = 1 if Excess>med

drop med

eststo v0 : quietly regress SYNCH cfr volatility liquidity size i.group_id i.yea ,cluster(name)
eststo v1 : quietly regress SYNCH Excess cfr size i.group_id i.year ,cluster(name)
eststo v2 : quietly regress SYNCH Excess cfr volatility liquidity size i.group_id i.year ,cluster(name)
eststo v3 : quietly regress SYNCH ExcessDiff cfr volatility liquidity size i.group_id i.year ,cluster(name)
eststo v4 : quietly regress SYNCH ExcessDummy cfr volatility liquidity size i.group_id i.year ,cluster(name)

eststo v5 : quietly regress SYNCH ExcessHigh cfr volatility liquidity size i.group_id i.year ,cluster(name)

esttab v0 v1 v2 v3 v4 v5, n r2  order(Excess  ExcessDiff ExcessDummy ExcessHigh cfr) keep(ExcessDummy  ExcessHigh ExcessDiff Excess cfr volatility liquidity size) nomtitle
 
 
xtset id year
  
 eststo v0 : quietly xi: asreg SYNCH volatility liquidity size i.group_id   , fmb newey(4)
 eststo v1 : quietly xi: asreg SYNCH Excess cfr  size i.group_id  , fmb newey(4)
  eststo v2 : quietly xi: asreg SYNCH Excess cfr volatility liquidity size i.group_id  , fmb newey(4)
eststo v3 : quietly xi: asreg SYNCH ExcessDiff cfr volatility liquidity size i.group_id , fmb newey(4)
eststo v4 : quietly xi: asreg SYNCH ExcessDummy cfr volatility liquidity size i.group_id  , fmb newey(4)

eststo v5 : quietly xi: asreg SYNCH ExcessHigh cfr volatility liquidity size i.group_id  , fmb newey(4)

esttab v0 v1 v2 v3 v4 v5, n r2  order(Excess  ExcessDiff ExcessDummy ExcessHigh cfr) keep(ExcessDummy  ExcessHigh ExcessDiff Excess cfr volatility liquidity size) 