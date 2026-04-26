# =============================================================================
# Easy Equities TFSA ETF  —  Data Pipeline
# =============================================================================
# CHANGELOG (latest)
#  - PRICE DATA: Yahoo Finance primary, Stooq fallback for missing tickers
#  - HOLDINGS: Diagnostic multi-source scraper (verbose error reporting)
#  - PARTIAL OUTPUT: ETFs without price data still get JSON with metadata
#                    + holdings + index rules (so they appear in tables)
#  - HOLDINGS_VERIFIED date — tells you how stale the fallback data is
#
# HONEST CAVEAT
#  Live scraping of JSE ETF holdings is unreliable from free sources
#  (Cloudflare, JS-rendered pages, paywalls). The hardcoded fallback is
#  the practical source of truth — review it quarterly after index rebalances.
#
# DEPENDENCIES
#   install.packages(c("tidyverse","tidyquant","quantmod","lubridate",
#                      "slider","jsonlite","glue","here","rvest","httr"))
# =============================================================================

suppressPackageStartupMessages({
  library(tidyverse)
  library(tidyquant)
  library(quantmod)
  library(lubridate)
  library(slider)
  library(jsonlite)
  library(glue)
  library(here)
  library(rvest)
  library(httr)
})

# ── Config ────────────────────────────────────────────────────────────────────

OUTPUT_DIR        <- here("posts", "ee-etf-tracker", "data")
START_DATE        <- as.Date("2019-01-01")
END_DATE          <- Sys.Date()
RISK_FREE_ANN     <- 0.0825
DAILY_RF          <- RISK_FREE_ANN / 252
MIN_ROWS          <- 20
HOLDINGS_VERIFIED <- "2025-01-15"   # update this date when you manually review holdings

UA <- paste0("Mozilla/5.0 (Windows NT 10.0; Win64; x64) ",
             "AppleWebKit/537.36 (KHTML, like Gecko) ",
             "Chrome/121.0.0.0 Safari/537.36")

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

# =============================================================================
# ETF CATALOGUE — all Easy Equities TFSA-eligible ETFs as of April 2025
# =============================================================================

etf_catalogue <- list(
  list(ticker="STX40",  yf="STX40.JO",  name="Satrix 40 ETF",
       provider="Satrix", category="SA Broad Equity", ter=0.0010, dividend="Pays",
       index="FTSE/JSE Top 40 Index",
       index_rules="Tracks the 40 largest JSE companies by investable market cap. No SWIX adjustment — full float-adjusted global market cap weighting. 10% individual stock cap. Reviewed quarterly by the FTSE/JSE Advisory Committee.",
       rebalancing="Quarterly"),
  list(ticker="STXSWX", yf="STXSWX.JO", name="Satrix SWIX Top 40 ETF",
       provider="Satrix", category="SA Broad Equity", ter=0.0010, dividend="Pays",
       index="FTSE/JSE SWIX Top 40 Index",
       index_rules="Same 40 constituents as Top 40 but weighted by shares held on the South African share register (STRATE), reducing exposure to dual-listed rand hedges like BHP and Richemont.",
       rebalancing="Quarterly"),
  list(ticker="STXCAP", yf="STXCAP.JO", name="Satrix Capped All Share ETF",
       provider="Satrix", category="SA Broad Equity", ter=0.0025, dividend="Pays",
       index="FTSE/JSE Capped All Share Index (CAPI)",
       index_rules="Tracks ~140 JSE All Share companies with a 10% per-stock cap. Broader than Top 40, capturing mid-cap JSE exposure.",
       rebalancing="Quarterly"),
  list(ticker="STXJGE", yf="STXJGE.JO", name="Satrix JSE Global Equity ETF",
       provider="Satrix", category="SA Broad Equity", ter=0.0015, dividend="Pays",
       index="FTSE/JSE Global Investor Index (J501)",
       index_rules="Listed March 2024. The 50 largest JSE-listed companies weighted by GLOBAL free-float market cap (not SWIX). This upweights dual-listed companies like BHP, Glencore and Richemont, providing the highest rand-hedge exposure of any JSE broad equity ETF. 10% cap.",
       rebalancing="Quarterly"),
  list(ticker="STXRAF", yf="STXRAF.JO", name="Satrix RAFI 40 ETF",
       provider="Satrix", category="SA Smart Beta", ter=0.0051, dividend="Pays",
       index="FTSE/JSE RAFI 40 Index",
       index_rules="Fundamental indexing — constituents ranked by dividends, cash flow, sales and book value rather than market cap. Creates a structural value tilt.",
       rebalancing="Annual"),
  list(ticker="ETFT40", yf="ETFT40.JO", name="1nvest Top 40 ETF",
       provider="1nvest", category="SA Broad Equity", ter=0.0010, dividend="Pays",
       index="FTSE/JSE Top 40 Index",
       index_rules="Identical benchmark to STX40. Managed by Standard Bank's 1nvest division.",
       rebalancing="Quarterly"),
  list(ticker="ETFSWX", yf="ETFSWX.JO", name="1nvest Capped SWIX ETF",
       provider="1nvest", category="SA Broad Equity", ter=0.0010, dividend="Pays",
       index="FTSE/JSE Capped SWIX All Share Index",
       index_rules="SWIX-weighted All Share universe with 10% single-stock cap. Combines SA-register weighting with broad equity coverage including mid-caps.",
       rebalancing="Quarterly"),
  list(ticker="CTOP50", yf="CTOP50.JO", name="10X Top 50 ETF",
       provider="10X (CoreShares)", category="SA Broad Equity", ter=0.0025, dividend="Pays",
       index="S&P South Africa Top 50 Index",
       index_rules="Tracks the 50 largest JSE companies by market cap with a 10% individual stock cap. Note: tracks S&P index, not FTSE/JSE.",
       rebalancing="Quarterly"),
  list(ticker="SYGT40", yf="SYGT40.JO", name="Sygnia Itrix Top 40 ETF",
       provider="Sygnia", category="SA Broad Equity", ter=0.0010, dividend="Pays",
       index="FTSE/JSE Top 40 Index",
       index_rules="Tracks the FTSE/JSE Top 40 Index. Managed by Sygnia. Pays dividends bi-annually.",
       rebalancing="Quarterly"),
  list(ticker="STXFIN", yf="STXFIN.JO", name="Satrix Financials ETF",
       provider="Satrix", category="SA Sector", ter=0.0010, dividend="Pays",
       index="FTSE/JSE Capped Financials 15 Index",
       index_rules="The 15 largest JSE financial sector companies. 15% per-stock cap.",
       rebalancing="Quarterly"),
  list(ticker="STXIND", yf="STXIND.JO", name="Satrix Capped Industrials ETF",
       provider="Satrix", category="SA Sector", ter=0.0010, dividend="Pays",
       index="FTSE/JSE Capped Industrials 25 Index",
       index_rules="25 JSE Industrial sector companies (consumer discretionary, media, telecoms). Naspers dominates. 20% per-stock cap.",
       rebalancing="Quarterly"),
  list(ticker="STXRES", yf="STXRES.JO", name="Satrix Capped Resources ETF",
       provider="Satrix", category="SA Sector", ter=0.0010, dividend="Pays",
       index="FTSE/JSE Capped Resources 10 Index",
       index_rules="The 10 largest JSE resources (mining) companies. 20% per-constituent cap.",
       rebalancing="Quarterly"),
  list(ticker="STXDIV", yf="STXDIV.JO", name="Satrix Divi Plus ETF",
       provider="Satrix", category="SA Dividend", ter=0.0010, dividend="Pays",
       index="FTSE/JSE Dividend Plus Index",
       index_rules="The 30 highest dividend-yielding shares from the All Share Index, weighted by yield.",
       rebalancing="Semi-annual"),
  list(ticker="STXQUA", yf="STXQUA.JO", name="Satrix Quality SA ETF",
       provider="Satrix", category="SA Smart Beta", ter=0.0020, dividend="Pays",
       index="S&P SA Quality Index",
       index_rules="SA stocks scored on ROE, accruals ratio and financial leverage. Top 20% quality stocks weighted by quality score × market cap.",
       rebalancing="Annual"),
  list(ticker="STXMMT", yf="STXMMT.JO", name="Satrix Momentum ETF",
       provider="Satrix", category="SA Smart Beta", ter=0.0020, dividend="Pays",
       index="S&P SA Momentum Index",
       index_rules="SA stocks with the strongest 12-month risk-adjusted price momentum (excluding most recent month).",
       rebalancing="Semi-annual"),
  list(ticker="STXLVL", yf="STXLVL.JO", name="Satrix Low Volatility ETF",
       provider="Satrix", category="SA Smart Beta", ter=0.0020, dividend="Pays",
       index="S&P SA Low Volatility Index",
       index_rules="The 30 least volatile SA stocks over trailing 252 days. Weighted inversely by volatility.",
       rebalancing="Quarterly"),
  list(ticker="STXPRO", yf="STXPRO.JO", name="Satrix Property ETF",
       provider="Satrix", category="SA Property", ter=0.0010, dividend="Pays",
       index="FTSE/JSE SA Listed Property Index (SAPY)",
       index_rules="All JSE-listed REITs and property companies by float-adjusted market cap, 20% per-stock cap.",
       rebalancing="Quarterly"),
  list(ticker="CSPROP", yf="CSPROP.JO", name="10X SA Property Income ETF",
       provider="10X (CoreShares)", category="SA Property", ter=0.0045, dividend="Pays",
       index="FTSE/JSE SA Listed Property Index (SAPY)",
       index_rules="Same benchmark as STXPRO, managed by 10X.",
       rebalancing="Quarterly"),
  list(ticker="STXILB", yf="STXILB.JO", name="Satrix ILBI ETF",
       provider="Satrix", category="SA Bond", ter=0.0025, dividend="Pays",
       index="FTSE/JSE ILBI Index",
       index_rules="SA government inflation-linked bonds weighted by market value. Real returns above CPI.",
       rebalancing="Monthly"),
  list(ticker="CSGOVI", yf="CSGOVI.JO", name="10X Wealth Govi Bond ETF",
       provider="10X (CoreShares)", category="SA Bond", ter=0.0024, dividend="Pays",
       index="FTSE/JSE GOVI Index",
       index_rules="SA government nominal bonds, weighted by outstanding market value.",
       rebalancing="Monthly"),
  list(ticker="PREFTX", yf="PREFTX.JO", name="10X Yield Selected Bond ETF",
       provider="10X (CoreShares)", category="SA Bond", ter=0.0035, dividend="Pays",
       index="Custom: 8 Highest-Yielding SA Government Bonds",
       index_rules="Previously the CoreShares PrefTrax (preference share tracker). Remandated April 2023 after the preference share market shrank due to Basel III. Now tracks a custom index of the 8 highest-yielding SA government bonds.",
       rebalancing="Quarterly"),
  list(ticker="STX500", yf="STX500.JO", name="Satrix S&P 500 Feeder ETF",
       provider="Satrix", category="Global Equity", ter=0.0035, dividend="Reinvests",
       index="S&P 500 Index",
       index_rules="Feeder into Vanguard S&P 500 UCITS ETF. 500 large US companies.",
       rebalancing="Quarterly"),
  list(ticker="SYG500", yf="SYG500.JO", name="Sygnia Itrix S&P 500 ETF",
       provider="Sygnia", category="Global Equity", ter=0.0015, dividend="Reinvests",
       index="S&P 500 Index",
       index_rules="Tracks the S&P 500 via iShares S&P 500 UCITS ETF. Lowest-cost S&P 500 product on the TFSA.",
       rebalancing="Quarterly"),
  list(ticker="ETF500", yf="ETF500.JO", name="1nvest S&P 500 Feeder ETF",
       provider="1nvest", category="Global Equity", ter=0.0020, dividend="Reinvests",
       index="S&P 500 Index",
       index_rules="Feeder into iShares Core S&P 500 UCITS ETF.",
       rebalancing="Quarterly"),
  list(ticker="CSP500", yf="CSP500.JO", name="10X S&P 500 ETF",
       provider="10X (CoreShares)", category="Global Equity", ter=0.0037, dividend="Reinvests",
       index="S&P 500 Index",
       index_rules="Feeder into iShares S&P 500 UCITS ETF, managed by 10X.",
       rebalancing="Quarterly"),
  list(ticker="ETF5IT", yf="ETF5IT.JO", name="1nvest S&P 500 Info Tech Feeder ETF",
       provider="1nvest", category="Global Equity", ter=0.0025, dividend="Reinvests",
       index="S&P 500 Information Technology Sector Index",
       index_rules="Only the IT sector of the S&P 500 (~70 companies). Concentrated in Apple, Microsoft, Nvidia.",
       rebalancing="Quarterly"),
  list(ticker="STXNDQ", yf="STXNDQ.JO", name="Satrix Nasdaq 100 Feeder ETF",
       provider="Satrix", category="Global Equity", ter=0.0048, dividend="Reinvests",
       index="Nasdaq-100 Index",
       index_rules="Feeder into Invesco Nasdaq-100 UCITS ETF. The 100 largest non-financial Nasdaq companies. Reviewed annually in December.",
       rebalancing="Annual (December)"),
  list(ticker="STXWDM", yf="STXWDM.JO", name="Satrix MSCI World Feeder ETF",
       provider="Satrix", category="Global Equity", ter=0.0035, dividend="Reinvests",
       index="MSCI World Index",
       index_rules="Feeder into Vanguard MSCI World UCITS ETF. ~1,500 large/mid caps across 23 developed markets. US ≈ 70%.",
       rebalancing="Quarterly"),
  list(ticker="SYGWD", yf="SYGWD.JO", name="Sygnia Itrix MSCI World ETF",
       provider="Sygnia", category="Global Equity", ter=0.0020, dividend="Reinvests",
       index="MSCI World Index",
       index_rules="Same benchmark as STXWDM via iShares Core MSCI World UCITS ETF.",
       rebalancing="Quarterly"),
  list(ticker="CTWRL", yf="CTWRL.JO", name="10X Total World ETF",
       provider="10X (CoreShares)", category="Global Equity", ter=0.0025, dividend="Reinvests",
       index="FTSE Global All Cap Index",
       index_rules="Feeder into Vanguard Total World Stock UCITS ETF. 9,500+ stocks across 49 countries — developed AND emerging markets plus small/mid caps. Broadest single-fund global exposure on the TFSA.",
       rebalancing="Quarterly"),
  list(ticker="STXEMG", yf="STXEMG.JO", name="Satrix MSCI Emerging Markets Feeder ETF",
       provider="Satrix", category="EM Equity", ter=0.0040, dividend="Reinvests",
       index="MSCI Emerging Markets Index",
       index_rules="Feeder into Vanguard MSCI EM UCITS ETF. ~1,400 large/mid caps in 24 EM countries.",
       rebalancing="Quarterly"),
  list(ticker="ASHGEQ", yf="ASHGEQ.JO", name="Ashburton Global 1200 ETF",
       provider="Ashburton (FNB)", category="Global Equity", ter=0.0075, dividend="Reinvests",
       index="S&P Global 1200 Index",
       index_rules="Composite of 7 regional indices covering 31 countries and ~1,200 companies.",
       rebalancing="Quarterly"),
  list(ticker="SYGJP", yf="SYGJP.JO", name="Sygnia Itrix MSCI Japan ETF",
       provider="Sygnia", category="Global Equity", ter=0.0020, dividend="Reinvests",
       index="MSCI Japan Index",
       index_rules="Tracks MSCI Japan via iShares Core MSCI Japan IMI UCITS ETF.",
       rebalancing="Quarterly"),
  list(ticker="SYGEU", yf="SYGEU.JO", name="Sygnia Itrix MSCI Europe ETF",
       provider="Sygnia", category="Global Equity", ter=0.0020, dividend="Reinvests",
       index="MSCI Europe Index",
       index_rules="Tracks MSCI Europe via iShares Core MSCI Europe UCITS ETF. ~430 companies across 15 European developed markets.",
       rebalancing="Quarterly"),
  list(ticker="SYGUK", yf="SYGUK.JO", name="Sygnia Itrix MSCI UK ETF",
       provider="Sygnia", category="Global Equity", ter=0.0020, dividend="Reinvests",
       index="MSCI United Kingdom Index",
       index_rules="Tracks MSCI UK via iShares Core MSCI UK IMI UCITS ETF. ~250 UK companies.",
       rebalancing="Quarterly"),
  list(ticker="GLPROP", yf="GLPROP.JO", name="10X S&P Global Property ETF",
       provider="10X (CoreShares)", category="Global Property", ter=0.0051, dividend="Pays",
       index="S&P Global Property 40 Index",
       index_rules="40 largest global REITs and real estate companies by float-adjusted market cap.",
       rebalancing="Quarterly"),
  list(ticker="ETFGPR", yf="ETFGPR.JO", name="1nvest Global REIT Index Feeder ETF",
       provider="1nvest", category="Global Property", ter=0.0025, dividend="Pays",
       index="FTSE EPRA/NAREIT Global REIT Index",
       index_rules="~300 REITs across 25 countries via iShares Global REIT ETF.",
       rebalancing="Quarterly"),
  list(ticker="GLODIV", yf="GLODIV.JO", name="10X S&P Global Dividend ETF",
       provider="10X (CoreShares)", category="Global Income", ter=0.0054, dividend="Pays",
       index="S&P Global Dividend Aristocrats Index",
       index_rules="Global stocks with 7+ consecutive years of non-decreasing dividends, weighted by dividend yield.",
       rebalancing="Annual"),
  list(ticker="ETFUSG", yf="ETFUSG.JO", name="1nvest ICE US Treasury Short Bond Feeder ETF",
       provider="1nvest", category="Global Bond", ter=0.0020, dividend="Pays",
       index="ICE US Treasury Short Bond Index",
       index_rules="Short-dated US Treasury bonds (1-3 year maturities). Low-risk USD-denominated income.",
       rebalancing="Monthly"),
  list(ticker="ETFGLD", yf="ETFGLD.JO", name="1nvest Gold ETF",
       provider="1nvest", category="Commodity", ter=0.0040, dividend="None",
       index="ZAR Gold Spot Price",
       index_rules="Physically-backed gold ETF. Each debenture = 1/100th troy oz gold in SA Reserve Bank vaults.",
       rebalancing="N/A"),
  list(ticker="NGPLT", yf="NGPLT.JO", name="NewPlat ETF",
       provider="Absa (NewGold)", category="Commodity", ter=0.0040, dividend="None",
       index="ZAR Platinum Spot Price",
       index_rules="Physically-backed platinum ETF. Each debenture = 1/100th troy oz physical platinum in SA vaults.",
       rebalancing="N/A"),
  list(ticker="ETFPLT", yf="ETFPLT.JO", name="1nvest Platinum ETF",
       provider="1nvest", category="Commodity", ter=0.0040, dividend="None",
       index="ZAR Platinum Spot Price",
       index_rules="Physically-backed platinum ETF managed by Standard Bank's 1nvest.",
       rebalancing="N/A"),
  list(ticker="ETFPLD", yf="ETFPLD.JO", name="1nvest Palladium ETF",
       provider="1nvest", category="Commodity", ter=0.0040, dividend="None",
       index="ZAR Palladium Spot Price",
       index_rules="Physically-backed palladium ETF. Supply concentrated in Russia and SA.",
       rebalancing="N/A"),
  list(ticker="ETFRHO", yf="ETFRHO.JO", name="1nvest Rhodium ETF",
       provider="1nvest", category="Commodity", ter=0.0040, dividend="None",
       index="ZAR Rhodium Spot Price",
       index_rules="Physically-backed rhodium ETF. Rarest PGM, used in catalytic converters.",
       rebalancing="N/A"),
  list(ticker="MAPPSG", yf="MAPPSG.JO", name="Satrix MAPPS Growth ETF",
       provider="Satrix", category="Multi-Asset", ter=0.0025, dividend="Pays",
       index="MAPPS Growth Index",
       index_rules="Multi-asset blend: SA Equity 75%, Nominal Bonds 10%, Inflation-Linked Bonds 10%, Cash 5%. Transferred from NewFunds to Satrix March 2023.",
       rebalancing="Monthly"),
  list(ticker="MAPPSP", yf="MAPPSP.JO", name="Satrix MAPPS Protect ETF",
       provider="Satrix", category="Multi-Asset", ter=0.0025, dividend="Pays",
       index="MAPPS Protect Index",
       index_rules="Conservative blend: SA Equity 40%, ILBs 35%, Nominal Bonds 15%, Cash 10%.",
       rebalancing="Monthly")
)

etf_df <- map_dfr(etf_catalogue, ~tibble(
  ticker=.x$ticker, yf=.x$yf, name=.x$name, provider=.x$provider,
  category=.x$category, ter=.x$ter, dividend=.x$dividend,
  index=.x$index, index_rules=.x$index_rules, rebalancing=.x$rebalancing
))

# =============================================================================
# HARDCODED HOLDINGS (verified manually — see HOLDINGS_VERIFIED above)
# =============================================================================

HOLDINGS_FALLBACK <- list(
  STX40   = list(Naspers=10.0,`BHP Group`=9.8,`Anglo American`=8.1,Glencore=7.9,
                 Richemont=7.2,`Gold Fields`=5.4,`Anglo Gold Ashanti`=4.9,
                 `Standard Bank`=4.6,FirstRand=4.2,`Anglo Platinum`=3.8),
  STXJGE  = list(`BHP Group`=10.0,Glencore=10.0,Richemont=10.0,`British American Tobacco`=9.8,
                 Naspers=8.4,`Anglo American`=7.2,Prosus=4.8,`Standard Bank`=3.9,
                 FirstRand=3.5,MTN=3.1),
  STXSWX  = list(`Standard Bank`=8.1,Naspers=7.9,FirstRand=7.2,`Absa Group`=5.8,
                 `Anglo American`=5.5,`MTN Group`=5.1,Nedbank=4.4,Sanlam=4.0,
                 `BHP Group`=3.9,Vodacom=3.6),
  STXCAP  = list(Naspers=10.0,`BHP Group`=7.8,`Anglo American`=6.1,Richemont=5.9,
                 `Standard Bank`=4.9,FirstRand=4.4,Glencore=4.1,`MTN Group`=3.7,
                 `Absa Group`=3.3,Prosus=2.9),
  STXRAF  = list(`Anglo American`=9.2,`BHP Group`=8.9,Naspers=7.4,`Standard Bank`=7.1,
                 FirstRand=6.5,Glencore=5.8,`Absa Group`=5.3,`MTN Group`=4.7,
                 Sanlam=3.9,Nedbank=3.6),
  ETFT40  = list(Naspers=10.0,`BHP Group`=9.8,`Anglo American`=8.1,Glencore=7.9,
                 Richemont=7.2,`Gold Fields`=5.4,`Anglo Gold Ashanti`=4.9,
                 `Standard Bank`=4.6,FirstRand=4.2,`Anglo Platinum`=3.8),
  ETFSWX  = list(`Standard Bank`=8.0,Naspers=7.8,FirstRand=7.1,`Absa Group`=5.7,
                 `Anglo American`=5.4,`MTN Group`=5.0,Nedbank=4.3,Sanlam=3.9,
                 `BHP Group`=3.8,Vodacom=3.5),
  CTOP50  = list(Naspers=10.0,`BHP Group`=8.2,`Anglo American`=6.5,Richemont=6.2,
                 `Standard Bank`=5.0,FirstRand=4.6,Glencore=4.3,`MTN Group`=3.8,
                 `Absa Group`=3.2,Prosus=2.9),
  SYGT40  = list(Naspers=10.0,`BHP Group`=9.8,`Anglo American`=8.1,Glencore=7.9,
                 Richemont=7.2,`Gold Fields`=5.4,`AngloGold Ashanti`=4.9,
                 `Standard Bank`=4.6,FirstRand=4.2,`Anglo Platinum`=3.8),
  STXFIN  = list(FirstRand=15.0,`Standard Bank`=15.0,`Absa Group`=14.2,Nedbank=12.8,
                 Sanlam=10.5,Discovery=8.8,`Old Mutual`=7.4,Momentum=5.9,
                 Capitec=5.6,RMH=4.8),
  STXIND  = list(Naspers=20.0,Richemont=18.9,`MTN Group`=12.6,Vodacom=10.3,
                 Prosus=8.8,Shoprite=6.2,`Pick n Pay`=4.1,Woolworths=3.9,
                 `Mr Price`=3.5,Truworths=2.9),
  STXRES  = list(`BHP Group`=20.0,`Anglo American`=20.0,Glencore=18.5,
                 `Anglo Platinum`=12.8,`Impala Platinum`=10.6,`Sibanye-Stillwater`=8.4,
                 South32=5.9,`African Rainbow`=3.8),
  STXDIV  = list(`Standard Bank`=8.9,FirstRand=8.5,`Absa Group`=7.8,Nedbank=7.2,
                 `BHP Group`=6.9,`Anglo American`=5.8,`MTN Group`=5.5,Sanlam=5.1,
                 Vodacom=4.8,Momentum=4.4),
  STXQUA  = list(Capitec=12.4,Richemont=11.8,Shoprite=9.6,Discovery=8.3,
                 FirstRand=7.9,`Clicks Group`=6.5,`Mr Price`=5.8,`Standard Bank`=5.2,
                 Naspers=4.9,Sanlam=4.1),
  STXMMT  = list(Naspers=15.3,Capitec=11.7,Richemont=10.2,`Anglo American`=8.9,
                 Shoprite=7.6,`Standard Bank`=6.8,`BHP Group`=6.1,FirstRand=5.4,
                 `Gold Fields`=4.8,Discovery=4.2),
  STXLVL  = list(Vodacom=8.4,Shoprite=7.9,`Clicks Group`=7.5,Nedbank=7.1,
                 Sanlam=6.8,`Old Mutual`=6.4,Momentum=6.0,Woolworths=5.7,
                 Bidvest=5.3,`Tiger Brands`=5.0),
  STXPRO  = list(Growthpoint=16.5,Redefine=14.2,Emira=10.8,Resilient=10.4,
                 `Fortress A`=9.6,Hyprop=8.9,`SA Corp`=6.7,Attacq=5.8,
                 `Investec Property`=4.9,Equites=4.5),
  CSPROP  = list(Growthpoint=16.5,Redefine=14.2,Emira=10.8,Resilient=10.4,
                 `Fortress A`=9.6,Hyprop=8.9,`SA Corp`=6.7,Attacq=5.8,
                 `Investec Property`=4.9,Equites=4.5),
  STXILB  = list(`R197 ILB (2023)`=18.5,`R210 ILB (2028)`=16.2,`R202 ILB (2033)`=14.8,
                 `R212 ILB (2038)`=13.7,`R218 ILB (2025)`=12.4,`R222 ILB (2031)`=11.9,
                 `I2025`=7.6,`I2038`=4.9),
  CSGOVI  = list(`R186 (2026)`=18.9,`R2030 (2030)`=16.5,`R2032 (2032)`=14.2,
                 `R2035 (2035)`=12.8,`R2037 (2037)`=11.6,`R2040 (2040)`=10.4,
                 `R2044 (2044)`=8.7,`R2048 (2048)`=6.9),
  PREFTX  = list(`RSA Govt Bond R186`=16.2,`RSA Govt Bond R2030`=15.8,
                 `RSA Govt Bond R213`=14.5,`RSA Govt Bond R2032`=13.9,
                 `RSA Govt Bond R2035`=12.6,`RSA Govt Bond R2037`=11.4,
                 `RSA Govt Bond R2040`=9.8,`RSA Govt Bond R2044`=5.8),
  STX500  = list(Apple=7.1,Microsoft=6.4,Nvidia=6.0,Amazon=3.9,Meta=2.8,
                 `Alphabet A`=2.1,`Alphabet C`=1.8,`Berkshire Hathaway`=1.7,
                 `Eli Lilly`=1.6,Broadcom=1.5),
  SYG500  = list(Apple=7.1,Microsoft=6.4,Nvidia=6.0,Amazon=3.9,Meta=2.8,
                 `Alphabet A`=2.1,`Alphabet C`=1.8,`Berkshire Hathaway`=1.7,
                 `Eli Lilly`=1.6,Broadcom=1.5),
  ETF500  = list(Apple=7.1,Microsoft=6.4,Nvidia=6.0,Amazon=3.9,Meta=2.8,
                 `Alphabet A`=2.1,`Alphabet C`=1.8,`Berkshire Hathaway`=1.7,
                 `Eli Lilly`=1.6,Broadcom=1.5),
  CSP500  = list(Apple=7.1,Microsoft=6.4,Nvidia=6.0,Amazon=3.9,Meta=2.8,
                 `Alphabet A`=2.1,`Alphabet C`=1.8,`Berkshire Hathaway`=1.7,
                 `Eli Lilly`=1.6,Broadcom=1.5),
  ETF5IT  = list(Apple=21.5,Microsoft=19.8,Nvidia=18.2,Broadcom=5.4,
                 `TSMC ADR`=3.1,Salesforce=2.8,AMD=2.5,Qualcomm=2.2,
                 Oracle=2.1,`Texas Instruments`=1.8),
  STXNDQ  = list(Apple=9.1,Microsoft=8.5,Nvidia=8.2,Amazon=5.3,Meta=4.8,
                 `Alphabet A`=3.4,Broadcom=3.1,Tesla=2.9,Costco=2.6,Netflix=2.3),
  STXWDM  = list(Apple=4.9,Microsoft=4.4,Nvidia=4.1,Amazon=2.7,Meta=1.9,
                 `Alphabet A`=1.5,`Berkshire Hathaway`=1.2,`Eli Lilly`=1.1,
                 Broadcom=1.0,LVMH=0.9),
  SYGWD   = list(Apple=4.9,Microsoft=4.4,Nvidia=4.1,Amazon=2.7,Meta=1.9,
                 `Alphabet A`=1.5,`Berkshire Hathaway`=1.2,`Eli Lilly`=1.1,
                 Broadcom=1.0,LVMH=0.9),
  CTWRL   = list(Apple=4.2,Microsoft=3.8,Nvidia=3.5,Amazon=2.3,Meta=1.6,
                 `Alphabet A`=1.3,`Taiwan Semiconductor`=1.0,`Berkshire Hathaway`=1.0,
                 `Eli Lilly`=0.9,Samsung=0.8),
  STXEMG  = list(`Taiwan Semiconductor`=8.5,`Samsung Electronics`=4.1,Tencent=3.8,
                 Alibaba=2.9,Meituan=2.4,`HDFC Bank`=2.1,`Reliance Industries`=2.0,
                 Infosys=1.8,`SK Hynix`=1.6,`ICICI Bank`=1.5),
  ASHGEQ  = list(Apple=4.5,Microsoft=4.0,Nvidia=3.8,Amazon=2.6,Meta=1.8,
                 `Alphabet A`=1.4,`Berkshire Hathaway`=1.1,ASML=0.9,
                 `Eli Lilly`=0.9,`Novo Nordisk`=0.8),
  SYGJP   = list(`Toyota Motor`=4.8,Sony=3.6,Keyence=3.1,`Recruit Holdings`=2.8,
                 `Shin-Etsu Chemical`=2.5,`Tokyo Electron`=2.3,KDDI=2.1,
                 `Honda Motor`=1.9,`SoftBank Group`=1.8,`Mitsubishi UFJ`=1.6),
  SYGEU   = list(ASML=4.2,`Nestlé`=3.8,LVMH=3.5,`Novo Nordisk`=3.3,SAP=2.9,
                 Roche=2.6,AstraZeneca=2.4,Shell=2.2,`Hermès`=2.0,Siemens=1.9),
  SYGUK   = list(AstraZeneca=9.8,Shell=8.5,HSBC=7.2,Unilever=6.4,BP=5.1,
                 `Rio Tinto`=4.8,GSK=4.4,Diageo=4.1,`BHP (UK)`=3.8,RELX=3.4),
  GLPROP  = list(Prologis=8.2,`American Tower`=7.4,Equinix=6.8,`Simon Property`=5.9,
                 `Crown Castle`=5.1,`Public Storage`=4.8,`Realty Income`=4.5,
                 Welltower=4.2,AvalonBay=3.8,`Digital Realty`=3.5),
  ETFGPR  = list(Prologis=8.5,`American Tower`=6.9,Equinix=6.4,`Simon Property`=5.5,
                 `Public Storage`=4.9,`Crown Castle`=4.7,`Realty Income`=4.2,
                 Welltower=3.9,Ventas=3.6,`Alexandria RE`=3.2),
  GLODIV  = list(`Fortescue Metals`=4.8,`Altria Group`=4.4,`T. Rowe Price`=3.9,
                 `Realty Income`=3.7,Enbridge=3.5,`AT&T`=3.1,`BCE Inc`=2.9,
                 `Kinder Morgan`=2.7,`Eversource Energy`=2.5,`Leggett & Platt`=2.3),
  ETFUSG  = list(`US Treasury 2Y`=19.5,`US Treasury 1Y`=18.2,`US Treasury 18M`=16.8,
                 `US Treasury 3Y`=15.1,`US T-Bill 6M`=12.6,`US T-Bill 3M`=10.9,
                 `US T-Bill 1M`=6.9),
  ETFGLD  = list(`Physical Gold (100%)`=100.0),
  NGPLT   = list(`Physical Platinum (100%)`=100.0),
  ETFPLT  = list(`Physical Platinum (100%)`=100.0),
  ETFPLD  = list(`Physical Palladium (100%)`=100.0),
  ETFRHO  = list(`Physical Rhodium (100%)`=100.0),
  MAPPSG  = list(`SA Equity Basket`=75.0,`SA Nominal Bonds`=10.0,
                 `SA Inflation Bonds`=10.0,`Cash / Money Market`=5.0),
  MAPPSP  = list(`SA Equity Basket`=40.0,`SA Inflation Bonds`=35.0,
                 `SA Nominal Bonds`=15.0,`Cash / Money Market`=10.0)
)

# =============================================================================
# HOLDINGS SCRAPER (diagnostic — shows real error reasons)
# =============================================================================

safe_get <- function(url, timeout = 20) {
  tryCatch(
    GET(url, timeout(timeout),
        add_headers(
          `User-Agent`      = UA,
          `Accept`          = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          `Accept-Language` = "en-US,en;q=0.9",
          `Accept-Encoding` = "gzip, deflate, br"
        )),
    error = function(e) structure(list(error = e$message), class = "fail")
  )
}

#' Try multiple HTML scraping sources, returning detailed diagnostics
scrape_holdings_live <- function(ticker) {
  attempts <- list()
  
  # ── Source 1: stockanalysis.com ─────────────────────────────────────────────
  url <- glue("https://stockanalysis.com/quote/jse/{ticker}/holdings/")
  resp <- safe_get(url)
  if (inherits(resp, "fail")) {
    attempts[[length(attempts)+1]] <- glue("stockanalysis: {resp$error}")
  } else {
    sc <- status_code(resp)
    if (sc != 200) {
      attempts[[length(attempts)+1]] <- glue("stockanalysis: HTTP {sc}")
    } else {
      page <- tryCatch(read_html(content(resp, "text", encoding = "UTF-8")), error = function(e) NULL)
      if (is.null(page)) {
        attempts[[length(attempts)+1]] <- "stockanalysis: HTML parse failed"
      } else {
        tbls <- tryCatch(html_elements(page, "table") |> map(html_table, fill = TRUE), error = function(e) list())
        tbl <- tbls |>
          keep(~any(str_detect(names(.x), regex("%|weight|assets", ignore_case = TRUE)))) |>
          first()
        if (is.null(tbl) || nrow(tbl) == 0) {
          attempts[[length(attempts)+1]] <- "stockanalysis: no holdings table found"
        } else {
          names(tbl) <- tolower(str_replace_all(names(tbl), "[^a-z0-9]", "_"))
          name_col <- names(tbl)[str_detect(names(tbl), "name|company|holding")][1]
          weight_col <- names(tbl)[str_detect(names(tbl), "asset|weight|alloc|pct|percent")][1]
          if (is.na(name_col)) name_col <- names(tbl)[2]
          if (is.na(weight_col)) weight_col <- names(tbl)[ncol(tbl)]
          h <- tbl |>
            select(name = all_of(name_col), weight = all_of(weight_col)) |>
            mutate(name = str_squish(as.character(name)),
                   weight = as.numeric(str_remove_all(as.character(weight), "[^0-9.]"))) |>
            filter(!is.na(weight), weight > 0, nchar(name) > 1) |>
            slice_max(weight, n = 10, with_ties = FALSE)
          if (nrow(h) >= 3) return(list(
            holdings = set_names(as.list(h$weight), h$name),
            source   = "stockanalysis.com",
            attempts = attempts
          ))
          attempts[[length(attempts)+1]] <- "stockanalysis: only got <3 holdings"
        }
      }
    }
  }
  
  # ── Source 2: Yahoo Finance holdings page ──────────────────────────────────
  url <- glue("https://finance.yahoo.com/quote/{ticker}.JO/holdings/")
  resp <- safe_get(url)
  if (inherits(resp, "fail")) {
    attempts[[length(attempts)+1]] <- glue("yahoo: {resp$error}")
  } else {
    sc <- status_code(resp)
    if (sc != 200) {
      attempts[[length(attempts)+1]] <- glue("yahoo: HTTP {sc}")
    } else {
      # Yahoo typically requires JS for holdings, but try anyway
      page <- tryCatch(read_html(content(resp, "text", encoding = "UTF-8")), error = function(e) NULL)
      if (is.null(page)) {
        attempts[[length(attempts)+1]] <- "yahoo: HTML parse failed"
      } else {
        # Look for holdings list with class or section pattern
        rows <- tryCatch(html_elements(page, "section[data-test='top-holdings'] tr, table.W\\(100\\%\\) tr"),
                         error = function(e) NULL)
        if (is.null(rows) || length(rows) < 3) {
          attempts[[length(attempts)+1]] <- "yahoo: no holdings rows found (likely JS-rendered)"
        } else {
          # Try to extract name/weight pairs
          h <- map_dfr(rows, ~{
            cells <- html_elements(.x, "td")
            if (length(cells) < 2) return(NULL)
            tibble(name = html_text2(cells[1]),
                   weight = as.numeric(str_remove_all(html_text2(cells[length(cells)]), "[^0-9.]")))
          }) |> filter(!is.na(weight), weight > 0, nchar(name) > 1) |>
            slice_max(weight, n = 10, with_ties = FALSE)
          if (nrow(h) >= 3) return(list(
            holdings = set_names(as.list(h$weight), h$name),
            source   = "finance.yahoo.com",
            attempts = attempts
          ))
          attempts[[length(attempts)+1]] <- "yahoo: extracted <3 valid holdings"
        }
      }
    }
  }
  
  # All sources failed
  return(list(holdings = NULL, source = NULL, attempts = attempts))
}

fetch_holdings <- function(ticker, verbose = FALSE) {
  message(glue("  Holdings [{ticker}]..."), appendLF = FALSE)
  
  # Single-asset / fixed-allocation funds — skip scraping
  if (ticker %in% c("ETFGLD","NGPLT","ETFPLT","ETFPLD","ETFRHO")) {
    message(" (physical asset)")
    return(HOLDINGS_FALLBACK[[ticker]])
  }
  if (ticker %in% c("MAPPSG","MAPPSP")) {
    message(" (fixed allocation)")
    return(HOLDINGS_FALLBACK[[ticker]])
  }
  
  # Live scrape attempt
  result <- tryCatch(scrape_holdings_live(ticker),
                     error = function(e) list(holdings = NULL, attempts = list(glue("crash: {e$message}"))))
  
  if (!is.null(result$holdings)) {
    message(glue(" ✓ {result$source} ({length(result$holdings)} holdings)"))
    return(result$holdings)
  }
  
  # All scraping failed — show why if verbose
  if (verbose && length(result$attempts) > 0) {
    message(" ⚠ fallback")
    walk(result$attempts, ~message(glue("      ↳ {.x}")))
  } else {
    message(glue(" ⚠ fallback (verified {HOLDINGS_VERIFIED})"))
  }
  
  # Cached previous result?
  cached_file <- file.path(OUTPUT_DIR, glue("{ticker}.json"))
  if (file.exists(cached_file)) {
    cached <- tryCatch(fromJSON(cached_file, simplifyDataFrame = FALSE), error = function(e) NULL)
    if (!is.null(cached$top_holdings) && length(cached$top_holdings) > 0 &&
        !is.null(cached$holdings_source) && cached$holdings_source != "fallback") {
      prev <- cached$top_holdings |>
        map(~setNames(list(.x$weight), .x$name)) |>
        reduce(c)
      return(prev)
    }
  }
  
  HOLDINGS_FALLBACK[[ticker]] %||% list()
}

# =============================================================================
# PRICE FETCHER — Yahoo Finance primary, Stooq fallback
# =============================================================================

fetch_prices <- function(yf_ticker, name) {
  # Attempt 1: Yahoo Finance via tidyquant
  result <- tryCatch({
    suppressWarnings(p <- tq_get(yf_ticker, from = START_DATE, to = END_DATE))
    if (is.null(p) || !is.data.frame(p) || nrow(p) < MIN_ROWS) NULL else p
  }, error = function(e) NULL)
  
  if (!is.null(result)) {
    message(glue("  Prices [{yf_ticker}]... ✓ Yahoo ({nrow(result)} rows)"))
    return(result)
  }
  
  # Attempt 2: Stooq via quantmod
  message(glue("  Prices [{yf_ticker}]... Yahoo failed, trying Stooq..."), appendLF = FALSE)
  result <- tryCatch({
    suppressWarnings(suppressMessages(
      x <- getSymbols(yf_ticker, src = "stooq", from = START_DATE, to = END_DATE,
                      auto.assign = FALSE)
    ))
    if (is.null(x) || nrow(x) < MIN_ROWS) NULL else {
      # Convert xts to tibble matching tidyquant format
      tibble(
        symbol   = yf_ticker,
        date     = as.Date(zoo::index(x)),
        open     = as.numeric(x[,1]),
        high     = as.numeric(x[,2]),
        low      = as.numeric(x[,3]),
        close    = as.numeric(x[,4]),
        volume   = as.numeric(x[,5]),
        adjusted = as.numeric(x[,4])  # Stooq doesn't provide adjusted, use close
      )
    }
  }, error = function(e) NULL)
  
  if (!is.null(result)) {
    message(glue(" ✓ Stooq ({nrow(result)} rows)"))
    return(result)
  }
  
  message(" ✗ both sources failed")
  NULL
}

# =============================================================================
# ANALYTICS
# =============================================================================

compute_metrics <- function(prices) {
  ret <- prices |> arrange(date) |>
    mutate(r = adjusted / lag(adjusted) - 1) |> filter(!is.na(r))
  n <- nrow(ret); yrs <- max(n / 252, 0.1)
  mu <- mean(ret$r); sig <- sd(ret$r)
  total <- prod(1 + ret$r) - 1
  cagr <- (1 + total)^(1/yrs) - 1
  vol <- sig * sqrt(252)
  sharpe <- if (sig > 0) (mu - DAILY_RF) / sig * sqrt(252) else NA_real_
  dd_sd <- sd(ret$r[ret$r < DAILY_RF])
  sortino <- if (is.finite(dd_sd) && dd_sd > 0) (mu - DAILY_RF) / dd_sd * sqrt(252) else NA_real_
  cum <- cumprod(1 + ret$r); peak <- cummax(cum); dd_vec <- cum / peak - 1
  max_dd <- min(dd_vec)
  calmar <- if (max_dd < 0) cagr / abs(max_dd) else NA_real_
  ups <- sum(pmax(ret$r - DAILY_RF, 0)); downs <- sum(pmax(DAILY_RF - ret$r, 0))
  omega <- if (downs > 0) ups / downs else NA_real_
  var95 <- quantile(ret$r, 0.05, names = FALSE)
  cvar95 <- mean(ret$r[ret$r <= var95])
  pr <- function(d) { s <- filter(ret, date >= Sys.Date()-d); if(nrow(s)<5) NA_real_ else prod(1+s$r)-1 }
  ytd <- { s <- filter(ret, date >= floor_date(Sys.Date(),"year")); if(nrow(s)>0) prod(1+s$r)-1 else NA_real_ }
  list(total_return=round(total,6), cagr=round(cagr,6), ann_vol=round(vol,6),
       sharpe=round(sharpe,4), sortino=round(sortino,4), calmar=round(calmar,4),
       omega=round(omega,4), max_drawdown=round(max_dd,6), var_95=round(var95,6),
       cvar_95=round(cvar95,6), ytd_return=round(ytd,6),
       return_1m=round(pr(30),6), return_3m=round(pr(91),6),
       return_6m=round(pr(182),6), return_1y=round(pr(365),6),
       return_3y=round(pr(1095),6), n_trading_days=n,
       start_date=format(min(ret$date)), end_date=format(max(ret$date)))
}

compute_monthly_returns <- function(prices) {
  prices |> arrange(date) |> mutate(ym=floor_date(date,"month")) |>
    group_by(ym) |> summarise(r=last(adjusted)/first(adjusted)-1,.groups="drop") |>
    transmute(year=year(ym), month_num=month(ym), return=round(r,6))
}

compute_rolling_vol <- function(prices) {
  ret <- prices |> arrange(date) |> mutate(r=adjusted/lag(adjusted)-1) |> filter(!is.na(r))
  for (w in c(30,90,252)) {
    nm <- paste0("vol_",w,"d")
    ret[[nm]] <- slide_dbl(ret$r, ~sd(.x)*sqrt(252), .before=w-1, .complete=TRUE)
  }
  ret |> mutate(week=floor_date(date,"week")) |> group_by(week) |>
    slice_tail(n=1) |> ungroup() |>
    select(date,vol_30d,vol_90d,vol_252d) |>
    mutate(across(where(is.numeric),~round(.x,6)))
}

compute_drawdown_series <- function(prices) {
  prices |> arrange(date) |>
    mutate(cum=adjusted/first(adjusted), peak=cummax(cum), drawdown=round(cum/peak-1,6)) |>
    mutate(week=floor_date(date,"week")) |> group_by(week) |>
    slice_tail(n=1) |> ungroup() |> select(date,drawdown)
}

build_price_series <- function(prices) {
  prices |> arrange(date) |> mutate(week=floor_date(date,"week")) |>
    group_by(week) |>
    summarise(date=last(date), open=first(open), high=max(high), low=min(low),
              close=last(close), volume=sum(volume), adjusted=last(adjusted), .groups="drop") |>
    mutate(daily_return=round(adjusted/lag(adjusted)-1,6),
           cum_return=round(adjusted/first(adjusted)-1,6)) |>
    select(-week) |> mutate(across(c(open,high,low,close,adjusted),~round(.x,4)))
}

# =============================================================================
# MAIN PIPELINE
# =============================================================================

# Set verbose=TRUE to see scraping diagnostics for every fallback
VERBOSE_HOLDINGS <- FALSE

message(glue(
  "\n{'='<70}\n Easy Equities TFSA ETF Pipeline  |  {format(Sys.time(),'%Y-%m-%d %H:%M')}\n",
  " ETFs       : {nrow(etf_df)}\n",
  " Dates      : {START_DATE} to {END_DATE}\n",
  " Output     : {OUTPUT_DIR}\n",
  " Holdings as of : {HOLDINGS_VERIFIED} (manual review)\n{'='<70}\n"
))

summary_rows <- list()
processed    <- character(0)
analytics_only <- character(0)
no_data      <- list()

for (i in seq_len(nrow(etf_df))) {
  row    <- etf_df[i, ]
  ticker <- row$ticker
  message(glue("\n[{i}/{nrow(etf_df)}] {ticker} — {row$name}"))
  
  # Holdings always attempted (independent of price availability)
  holdings_raw <- fetch_holdings(ticker, verbose = VERBOSE_HOLDINGS)
  holdings_arr <- imap(holdings_raw, ~list(name=.y, weight=.x)) |>
    keep(~nchar(.x$name)>0 && is.finite(.x$weight) && .x$weight>0) |>
    unname()
  
  # Try to fetch price data
  prices <- fetch_prices(row$yf, row$name)
  
  if (is.null(prices)) {
    # No price data → write metadata-only JSON so the ETF still appears in tables
    no_data[[ticker]] <- row$yf
    payload <- list(
      ticker=ticker, name=row$name, provider=row$provider,
      category=row$category, ter=row$ter, dividend=row$dividend,
      index=row$index, index_rules=row$index_rules, rebalancing=row$rebalancing,
      top_holdings=holdings_arr, has_price_data=FALSE,
      last_updated=format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    )
    write_json(payload, file.path(OUTPUT_DIR, glue("{ticker}.json")),
               auto_unbox=TRUE, digits=6, pretty=FALSE)
    summary_rows[[ticker]] <- tibble(
      ticker=ticker, name=row$name, provider=row$provider,
      category=row$category, ter=row$ter, dividend=row$dividend,
      ytd_return=NA_real_, return_1y=NA_real_, return_3y=NA_real_,
      cagr=NA_real_, ann_vol=NA_real_, sharpe=NA_real_, sortino=NA_real_,
      calmar=NA_real_, max_drawdown=NA_real_, var_95=NA_real_,
      n_trading_days=0L, last_updated=format(Sys.time(), "%Y-%m-%d")
    )
    processed <- c(processed, ticker)
    next
  }
  
  # Full analytics path
  metrics  <- compute_metrics(prices)
  monthly  <- compute_monthly_returns(prices)
  rolling  <- compute_rolling_vol(prices)
  drawdown <- compute_drawdown_series(prices)
  price_ts <- build_price_series(prices)
  
  payload <- list(
    ticker=ticker, name=row$name, provider=row$provider,
    category=row$category, ter=row$ter, dividend=row$dividend,
    index=row$index, index_rules=row$index_rules, rebalancing=row$rebalancing,
    top_holdings=holdings_arr, has_price_data=TRUE,
    metrics=metrics, prices=price_ts, monthly_returns=monthly,
    rolling_vol=rolling, drawdown=drawdown,
    last_updated=format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
  write_json(payload, file.path(OUTPUT_DIR, glue("{ticker}.json")),
             auto_unbox=TRUE, digits=6, pretty=FALSE)
  
  processed <- c(processed, ticker)
  summary_rows[[ticker]] <- tibble(
    ticker=ticker, name=row$name, provider=row$provider,
    category=row$category, ter=row$ter, dividend=row$dividend,
    ytd_return=metrics$ytd_return, return_1y=metrics$return_1y,
    return_3y=metrics$return_3y, cagr=metrics$cagr,
    ann_vol=metrics$ann_vol, sharpe=metrics$sharpe,
    sortino=metrics$sortino, calmar=metrics$calmar,
    max_drawdown=metrics$max_drawdown, var_95=metrics$var_95,
    n_trading_days=metrics$n_trading_days,
    last_updated=format(Sys.time(), "%Y-%m-%d")
  )
}

summary_df <- bind_rows(summary_rows)
write_json(summary_df,  file.path(OUTPUT_DIR,"summary.json"),  auto_unbox=TRUE, digits=6)
write_json(etf_df |> filter(ticker %in% processed),
           file.path(OUTPUT_DIR,"etf_list.json"), auto_unbox=TRUE, pretty=TRUE)

# Final report
message(glue("\n{'='<70}\n Pipeline complete  |  {format(Sys.time(),'%Y-%m-%d %H:%M:%S')}"))
message(glue(" Total ETFs : {nrow(etf_df)}"))
message(glue(" With prices: {length(processed) - length(no_data)} (full analytics)"))
message(glue(" No prices  : {length(no_data)} (metadata + holdings only)"))
if (length(no_data) > 0) {
  message(" Tickers without price data:")
  iwalk(no_data, ~message(glue("   {.y} ({.x})")))
}
message(glue("{'='<70}\n"))
message("To see scraping diagnostics, set VERBOSE_HOLDINGS <- TRUE near the top.")

# =============================================================================
# AUTO GIT COMMIT  (uncomment to enable)
# =============================================================================
# system(glue('cd "{here()}" && git add posts/ee-etf-tracker/data/ && ',
#             'git commit -m "data: weekly ETF refresh {Sys.Date()}" && ',
#             'git push'))