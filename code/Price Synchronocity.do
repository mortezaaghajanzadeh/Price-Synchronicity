cls
clear
// Osati and Aghajanzadeh
import delimited "G:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\Price Synchronocity\priceSynchronocity.csv", encoding(UTF-8) 


cd "D:\Dropbox\Finance(Prof.Heidari-Aghajanzadeh)\Project\Price-Synchronocity\report"

drop if year == 1399



gen volumeratio = volume /shrout

gen SYNCH = log(rsquared) - log(1-rsquared)

label variable SYNCH "SYNCH"

sum  SYNCH if year != 1398

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

gen leverage = debt/bookvalue

replace centrality = log(centrality) - log(1-centrality) 

label variable centrality " $ \ln(\frac{\text{centrality}}{1-\text{centrality}}) $"
replace noind = log(noind)
label variable noind " $ \ln(NIND) $"


gen crash = 0

replace crash = 1 if ncrash >0

logit crash Excess  volatility liquidity size leverage noind return_market skew kurt return_industry_std  volumeratio   i.group_id i.year if noind>1 ,cluster(name)



eststo v0 : quietly regress SYNCH  volatility liquidity size leverage noind return_market skew kurt return_industry_std  volumeratio   i.group_id i.year if noind>1 ,cluster(name)
estadd loc IndustryDummy "Yes" , replace
estadd loc YearDummy "Yes" , replace

eststo v1 : quietly regress SYNCH Excess cfr  i.group_id i.year if noind>1,cluster(name)
estadd loc IndustryDummy "Yes" , replace
estadd loc YearDummy "Yes" , replace

eststo v2 : quietly regress SYNCH Excess cfr volatility liquidity size leverage noind return_market skew kurt return_industry_std  volumeratio i.group_id i.year if noind>1,cluster(name)
estadd loc IndustryDummy "Yes" , replace
estadd loc YearDummy "Yes" , replace

eststo v3 : quietly regress SYNCH ExcessDiff cfr volatility liquidity size leverage noind return_market skew kurt return_industry_std  volumeratio i.group_id i.year if noind>1,cluster(name)
estadd loc IndustryDummy "Yes" , replace
estadd loc YearDummy "Yes" , replace

eststo v4 : quietly regress SYNCH ExcessDummy cfr volatility liquidity size leverage noind return_market skew kurt return_industry_std  volumeratio i.group_id i.year if noind>1,cluster(name)
estadd loc IndustryDummy "Yes" , replace
estadd loc YearDummy "Yes" , replace


eststo v5 : quietly regress SYNCH ExcessHigh cfr volatility liquidity size leverage noind return_market skew kurt return_industry_std  volumeratio i.group_id i.year if noind>1,cluster(name)
estadd loc IndustryDummy "Yes" , replace
estadd loc YearDummy "Yes" , replace

eststo v6 : quietly regress SYNCH position cfr volatility liquidity size leverage noind return_market skew kurt return_industry_std  volumeratio i.group_id i.year if noind>1,cluster(name)
estadd loc IndustryDummy "Yes" , replace
estadd loc YearDummy "Yes" , replace

eststo v7 : quietly regress SYNCH centrality cfr volatility liquidity size leverage noind return_market skew kurt return_industry_std  volumeratio i.group_id i.year if noind>1,cluster(name)
estadd loc IndustryDummy "Yes" , replace
estadd loc YearDummy "Yes" , replace


 

esttab v0 v1 v2 v3 v4 v5 v6 v7, label s(IndustryDummy YearDummy N  r2 ,   lab("Industry Dummy" "Year Dummy" "Observations""$ R^2 $")) brackets order(Excess  ExcessDiff ExcessDummy ExcessHigh position centrality cfr) keep(ExcessDummy  ExcessHigh ExcessDiff Excess position centrality cfr volatility liquidity size leverage noind return_market skew kurt return_industry_std  volumeratio) nomtitle


 mgroups("Synchronicity"   , pattern(1 ) prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span}) ) ,using synchronicityt4.tex ,replace
 
 
xtset id year
  
 eststo v0 : quietly  xi: asreg SYNCH volatility liquidity size leverage noind return_market skew kurt return_industry_std  volumeratio i.group_id  if noind>1 , fmb newey(4)
estadd loc IndustryDummy "Yes" , replace
estadd loc YearDummy "No" , replace

 eststo v1 : quietly xi: asreg SYNCH Excess cfr   i.group_id  if noind>1, fmb newey(4)
estadd loc IndustryDummy "Yes" , replace
estadd loc YearDummy "No" , replace

  eststo v2 : quietly xi: asreg SYNCH Excess cfr volatility liquidity size leverage noind i.group_id  if noind>1, fmb newey(4)
estadd loc IndustryDummy "Yes" , replace
estadd loc YearDummy "No" , replace

eststo v3 : quietly xi: asreg SYNCH ExcessDiff cfr volatility liquidity size leverage noind return_market skew kurt return_industry_std  volumeratio i.group_id if noind>1, fmb newey(4)
estadd loc IndustryDummy "Yes" , replace
estadd loc YearDummy "No" , replace

eststo v4 : quietly xi: asreg SYNCH ExcessDummy cfr volatility liquidity size leverage noind return_market skew kurt return_industry_std  volumeratio i.group_id  if noind>1, fmb newey(4)
estadd loc IndustryDummy "Yes" , replace
estadd loc YearDummy "No" , replace


eststo v5 : quietly xi: asreg SYNCH ExcessHigh cfr volatility liquidity size leverage noind return_market skew kurt return_industry_std  volumeratio i.group_id  if noind>1, fmb newey(4)
estadd loc IndustryDummy "Yes" , replace
estadd loc YearDummy "No" , replace


eststo v6 : quietly xi: asreg SYNCH position cfr volatility liquidity size leverage noind return_market skew kurt return_industry_std  volumeratio i.group_id  if noind>1, fmb newey(4)
estadd loc IndustryDummy "Yes" , replace
estadd loc YearDummy "No" , replace


eststo v7 : quietly xi: asreg SYNCH centrality cfr volatility liquidity size leverage noind return_market skew kurt return_industry_std  volumeratio i.group_id  if noind>1, fmb newey(4)
estadd loc IndustryDummy "Yes" , replace
estadd loc YearDummy "No" , replace


esttab v0 v1 v2 v3 v4 v5 v6 v7, brackets label s(IndustryDummy YearDummy N  r2 ,  lab("Industry Dummy" "Year Dummy" "Observations""$ R^2 $")) order(Excess  ExcessDiff ExcessDummy ExcessHigh position centrality cfr) keep(ExcessDummy  ExcessHigh ExcessDiff Excess position centrality cfr volatility liquidity size leverage noind return_market skew kurt return_industry_std  volumeratio)  nomtitle 

mgroups("Synchronicity"   , pattern(1 ) prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span}) ) ,using synchronicityt5.tex ,replace



