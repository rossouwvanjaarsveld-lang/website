# =============================================================================
# Easy Equities TFSA ETF  —  Data Pipeline  (Full Universe)
# =============================================================================
# Run BEFORE rendering the Quarto post.
# Writes JSON files to:  posts/etf-analytics/data/
#
# DEPENDENCIES
#   install.packages(c("tidyverse","tidyquant","lubridate","slider",
#                      "jsonlite","glue","here"))
# =============================================================================

suppressPackageStartupMessages({
  library(tidyverse)
  library(tidyquant)
  library(lubridate)
  library(slider)
  library(jsonlite)
  library(glue)
  library(here)
})

# ── Config ────────────────────────────────────────────────────────────────────

OUTPUT_DIR    <- here("posts", "etf-analytics", "data")
START_DATE    <- as.Date("2019-01-01")
END_DATE      <- Sys.Date()
RISK_FREE_ANN <- 0.0825   # SA repo rate — update periodically
DAILY_RF      <- RISK_FREE_ANN / 252

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

# =============================================================================
# ETF MASTER CATALOGUE — Full Easy Equities TFSA universe (2025)
# =============================================================================

etf_catalogue <- list(

  # ── SA BROAD EQUITY ─────────────────────────────────────────────────────────

  list(
    ticker = "STX40", yf_ticker = "STX40.JO",
    name = "Satrix 40 ETF", provider = "Satrix",
    category = "SA Broad Equity", ter = 0.0010, dividend = "Pays",
    index = "FTSE/JSE Top 40 Index",
    index_rules = "Tracks the 40 largest JSE companies by investable market cap. Constituents capped at 10%. The Shareholder Weighted (SWIX) methodology is NOT used — full float-adjusted market cap. Reviewed quarterly by the FTSE/JSE Advisory Committee.",
    rebalancing = "Quarterly",
    top_holdings = list(Naspers=22.1,`BHP Group`=8.4,`Anglo American`=6.8,Richemont=6.5,
                        `Standard Bank`=5.2,FirstRand=4.8,Glencore=4.6,`MTN Group`=3.9,
                        `Absa Group`=3.5,Prosus=3.1)
  ),

  list(
    ticker = "STXSWX", yf_ticker = "STXSWX.JO",
    name = "Satrix SWIX Top 40 ETF", provider = "Satrix",
    category = "SA Broad Equity", ter = 0.0010, dividend = "Pays",
    index = "FTSE/JSE SWIX Top 40 Index",
    index_rules = "Same 40 companies as the Top 40, but weighted by shares held on the South African share register (STRATE) rather than full global market cap. Reduces the weight of dual-listed rand hedges like BHP and Richemont, giving a more domestically-focused exposure.",
    rebalancing = "Quarterly",
    top_holdings = list(`Standard Bank`=8.1,Naspers=7.9,FirstRand=7.2,`Absa Group`=5.8,
                        `Anglo American`=5.5,`MTN Group`=5.1,Nedbank=4.4,Sanlam=4.0,
                        `BHP Group`=3.9,Vodacom=3.6)
  ),

  list(
    ticker = "STXCAP", yf_ticker = "STXCAP.JO",
    name = "Satrix Capped All Share ETF", provider = "Satrix",
    category = "SA Broad Equity", ter = 0.0025, dividend = "Pays",
    index = "FTSE/JSE Capped All Share Index (CAPI)",
    index_rules = "Tracks ~140 JSE-listed companies in the All Share Index with a 10% individual stock cap. Broader than Top 40, capturing mid-cap JSE exposure. Reviewed quarterly.",
    rebalancing = "Quarterly",
    top_holdings = list(Naspers=10.0,`BHP Group`=7.8,`Anglo American`=6.1,Richemont=5.9,
                        `Standard Bank`=4.9,FirstRand=4.4,Glencore=4.1,`MTN Group`=3.7,
                        `Absa Group`=3.3,Prosus=2.9)
  ),

  list(
    ticker = "STXRAF", yf_ticker = "STXRAF.JO",
    name = "Satrix RAFI 40 ETF", provider = "Satrix",
    category = "SA Smart Beta", ter = 0.0051, dividend = "Pays",
    index = "FTSE/JSE RAFI 40 Index",
    index_rules = "Fundamentally weighted — constituents ranked by 4 factors: dividends, cash flow, sales and book value, NOT market cap. Creates a structural value tilt. Overweights profitable companies relative to their share price, rebalancing away from overvalued stocks annually.",
    rebalancing = "Annual",
    top_holdings = list(`Anglo American`=9.2,`BHP Group`=8.9,Naspers=7.4,`Standard Bank`=7.1,
                        FirstRand=6.5,Glencore=5.8,`Absa Group`=5.3,`MTN Group`=4.7,
                        Sanlam=3.9,Nedbank=3.6)
  ),

  list(
    ticker = "ETFT40", yf_ticker = "ETFT40.JO",
    name = "1nvest Top 40 ETF", provider = "1nvest",
    category = "SA Broad Equity", ter = 0.0010, dividend = "Pays",
    index = "FTSE/JSE Top 40 Index",
    index_rules = "Identical benchmark to STX40 — tracks the 40 largest JSE companies by investable market cap with a 10% cap. Managed by Standard Bank's 1nvest division. Reviewed quarterly.",
    rebalancing = "Quarterly",
    top_holdings = list(Naspers=22.1,`BHP Group`=8.4,`Anglo American`=6.8,Richemont=6.5,
                        `Standard Bank`=5.2,FirstRand=4.8,Glencore=4.6,`MTN Group`=3.9,
                        `Absa Group`=3.5,Prosus=3.1)
  ),

  list(
    ticker = "ETFSWX", yf_ticker = "ETFSWX.JO",
    name = "1nvest Capped SWIX ETF", provider = "1nvest",
    category = "SA Broad Equity", ter = 0.0010, dividend = "Pays",
    index = "FTSE/JSE Capped SWIX All Share Index",
    index_rules = "Tracks the SWIX-weighted All Share universe (broader than SWIX 40) with a 10% single-stock cap. Combines SA-register weighting with broad equity exposure including mid-caps. Reviewed quarterly.",
    rebalancing = "Quarterly",
    top_holdings = list(`Standard Bank`=8.0,Naspers=7.8,FirstRand=7.1,`Absa Group`=5.7,
                        `Anglo American`=5.4,`MTN Group`=5.0,Nedbank=4.3,Sanlam=3.9,
                        `BHP Group`=3.8,Vodacom=3.5)
  ),

  list(
    ticker = "CTOP50", yf_ticker = "CTOP50.JO",
    name = "10X Top 50 ETF", provider = "10X (CoreShares)",
    category = "SA Broad Equity", ter = 0.0025, dividend = "Pays",
    index = "FTSE/JSE Top 50 Capped Index",
    index_rules = "Tracks the 50 largest JSE companies by market cap with a 10% individual stock cap. Slightly broader than Top 40 products by including 10 additional mid-large cap companies. Reviewed quarterly.",
    rebalancing = "Quarterly",
    top_holdings = list(Naspers=10.0,`BHP Group`=8.2,`Anglo American`=6.5,Richemont=6.2,
                        `Standard Bank`=5.0,FirstRand=4.6,Glencore=4.3,`MTN Group`=3.8,
                        `Absa Group`=3.2,Prosus=2.9)
  ),

  # ── SA SECTOR ───────────────────────────────────────────────────────────────

  list(
    ticker = "STXFIN", yf_ticker = "STXFIN.JO",
    name = "Satrix Financials ETF", provider = "Satrix",
    category = "SA Sector", ter = 0.0010, dividend = "Pays",
    index = "FTSE/JSE Capped Financials 15 Index",
    index_rules = "Tracks the 15 largest financial sector companies on the JSE: banks, insurance companies and asset managers. Constituents are weighted by investable market cap with a 15% per-stock cap. Reviewed quarterly.",
    rebalancing = "Quarterly",
    top_holdings = list(FirstRand=15.0,`Standard Bank`=15.0,`Absa Group`=14.2,Nedbank=12.8,
                        Sanlam=10.5,Discovery=8.8,`Old Mutual`=7.4,Momentum=5.9,
                        Capitec=5.6,RMH=4.8)
  ),

  list(
    ticker = "STXIND", yf_ticker = "STXIND.JO",
    name = "Satrix Capped Industrials ETF", provider = "Satrix",
    category = "SA Sector", ter = 0.0010, dividend = "Pays",
    index = "FTSE/JSE Capped Industrials 25 Index",
    index_rules = "Tracks 25 companies in the JSE Industrial classification, which includes consumer discretionary, media, and telecoms. Naspers dominates due to its Tencent/Prosus exposure. 20% per-stock cap. Reviewed quarterly.",
    rebalancing = "Quarterly",
    top_holdings = list(Naspers=20.0,Richemont=18.9,`MTN Group`=12.6,Vodacom=10.3,
                        Prosus=8.8,Shoprite=6.2,`Pick n Pay`=4.1,Woolworths=3.9,
                        `Mr Price`=3.5,Truworths=2.9)
  ),

  list(
    ticker = "STXRES", yf_ticker = "STXRES.JO",
    name = "Satrix Capped Resources ETF", provider = "Satrix",
    category = "SA Sector", ter = 0.0010, dividend = "Pays",
    index = "FTSE/JSE Capped Resources 10 Index",
    index_rules = "Tracks the 10 largest resources (mining) companies on the JSE by investable market cap. Heavily skewed toward global diversified miners with dual listings. 20% per-constituent cap. Reviewed quarterly.",
    rebalancing = "Quarterly",
    top_holdings = list(`BHP Group`=20.0,`Anglo American`=20.0,Glencore=18.5,
                        `Anglo Platinum`=12.8,`Impala Platinum`=10.6,`Sibanye-Stillwater`=8.4,
                        South32=5.9,`African Rainbow`=3.8,`Kumba Iron Ore`=0.0,`Gold Fields`=0.0)
  ),

  # ── SA SMART BETA / FACTOR ──────────────────────────────────────────────────

  list(
    ticker = "STXDIV", yf_ticker = "STXDIV.JO",
    name = "Satrix Divi Plus ETF", provider = "Satrix",
    category = "SA Dividend", ter = 0.0010, dividend = "Pays",
    index = "FTSE/JSE Dividend Plus Index",
    index_rules = "Selects the 30 highest dividend-yielding shares from the All Share Index, excluding REITs and preference shares. Constituents are weighted by dividend yield (highest yielders get the largest allocation). Must have paid a dividend in the past 12 months. Reviewed semi-annually.",
    rebalancing = "Semi-annual",
    top_holdings = list(`Standard Bank`=8.9,FirstRand=8.5,`Absa Group`=7.8,Nedbank=7.2,
                        `BHP Group`=6.9,`Anglo American`=5.8,`MTN Group`=5.5,Sanlam=5.1,
                        Vodacom=4.8,Momentum=4.4)
  ),

  list(
    ticker = "STXQUA", yf_ticker = "STXQUA.JO",
    name = "Satrix Quality SA ETF", provider = "Satrix",
    category = "SA Smart Beta", ter = 0.0020, dividend = "Pays",
    index = "S&P SA Quality Index",
    index_rules = "Selects SA stocks with high quality scores derived from 3 factors: return on equity (ROE), accruals ratio (earnings quality) and financial leverage. Targets the top 20% quality stocks in the SA universe, weighted by quality score × market cap. Reviewed annually.",
    rebalancing = "Annual",
    top_holdings = list(Capitec=12.4,Richemont=11.8,Shoprite=9.6,Discovery=8.3,
                        FirstRand=7.9,`Clicks Group`=6.5,`Mr Price`=5.8,`Standard Bank`=5.2,
                        Naspers=4.9,Sanlam=4.1)
  ),

  list(
    ticker = "STXMMT", yf_ticker = "STXMMT.JO",
    name = "Satrix Momentum ETF", provider = "Satrix",
    category = "SA Smart Beta", ter = 0.0020, dividend = "Pays",
    index = "S&P SA Momentum Index",
    index_rules = "Selects SA stocks with the strongest risk-adjusted price momentum over a 12-month lookback window (excluding the most recent month to avoid reversal). Weighted by momentum score × market cap. Reviewed semi-annually to capture momentum shifts.",
    rebalancing = "Semi-annual",
    top_holdings = list(Naspers=15.3,Capitec=11.7,Richemont=10.2,`Anglo American`=8.9,
                        Shoprite=7.6,`Standard Bank`=6.8,`BHP Group`=6.1,FirstRand=5.4,
                        `Gold Fields`=4.8,Discovery=4.2)
  ),

  list(
    ticker = "STXLVL", yf_ticker = "STXLVL.JO",
    name = "Satrix Low Volatility ETF", provider = "Satrix",
    category = "SA Smart Beta", ter = 0.0020, dividend = "Pays",
    index = "S&P SA Low Volatility Index",
    index_rules = "Selects the 30 least volatile SA stocks based on trailing 252-day realised volatility. Weighted inversely by volatility — the lower the vol, the higher the weight. Designed for capital preservation and smoother drawdown profile. Reviewed quarterly.",
    rebalancing = "Quarterly",
    top_holdings = list(Vodacom=8.4,Shoprite=7.9,`Clicks Group`=7.5,Nedbank=7.1,
                        Sanlam=6.8,`Old Mutual`=6.4,Momentum=6.0,Woolworths=5.7,
                        Bidvest=5.3,`Tiger Brands`=5.0)
  ),

  # ── SA PROPERTY ─────────────────────────────────────────────────────────────

  list(
    ticker = "STXPRO", yf_ticker = "STXPRO.JO",
    name = "Satrix Property ETF", provider = "Satrix",
    category = "SA Property", ter = 0.0010, dividend = "Pays",
    index = "FTSE/JSE SA Listed Property Index (SAPY)",
    index_rules = "Tracks all JSE-listed REITs and property companies, weighted by float-adjusted market cap with a 20% per-stock cap. Mandatory quarterly dividend distributions. Reviewed quarterly.",
    rebalancing = "Quarterly",
    top_holdings = list(Growthpoint=16.5,Redefine=14.2,Emira=10.8,Resilient=10.4,
                        `Fortress A`=9.6,Hyprop=8.9,`SA Corp`=6.7,Attacq=5.8,
                        `Investec Property`=4.9,Equites=4.5)
  ),

  list(
    ticker = "CSPROP", yf_ticker = "CSPROP.JO",
    name = "10X SA Property Income ETF", provider = "10X (CoreShares)",
    category = "SA Property", ter = 0.0045, dividend = "Pays",
    index = "FTSE/JSE SA Listed Property Index (SAPY)",
    index_rules = "Same benchmark as STXPRO — tracks JSE-listed REITs and property companies by market cap with a 20% single-stock cap. Managed by the 10X (formerly CoreShares) team. Reviewed quarterly.",
    rebalancing = "Quarterly",
    top_holdings = list(Growthpoint=16.5,Redefine=14.2,Emira=10.8,Resilient=10.4,
                        `Fortress A`=9.6,Hyprop=8.9,`SA Corp`=6.7,Attacq=5.8,
                        `Investec Property`=4.9,Equites=4.5)
  ),

  # ── SA BONDS ────────────────────────────────────────────────────────────────

  list(
    ticker = "STXILB", yf_ticker = "STXILB.JO",
    name = "Satrix ILBI ETF", provider = "Satrix",
    category = "SA Bond", ter = 0.0025, dividend = "Pays",
    index = "FTSE/JSE ILBI Index",
    index_rules = "Tracks SA government inflation-linked bonds (ILBs), weighted by market value. Provides returns above CPI inflation. The real yield is locked in at purchase but the capital value adjusts with CPI. Duration is medium-to-long. Reviewed monthly by the JSE.",
    rebalancing = "Monthly",
    top_holdings = list(`R197 ILB (2023)`=18.5,`R210 ILB (2028)`=16.2,`R202 ILB (2033)`=14.8,
                        `R212 ILB (2038)`=13.7,`R218 ILB (2025)`=12.4,`R222 ILB (2031)`=11.9,
                        `I2025`=7.6,`I2038`=4.9,`SA Govt ILB`=0.0,`SA Govt ILB`=0.0)
  ),

  list(
    ticker = "CSGOVI", yf_ticker = "CSGOVI.JO",
    name = "10X Wealth Govi Bond ETF", provider = "10X (CoreShares)",
    category = "SA Bond", ter = 0.0024, dividend = "Pays",
    index = "FTSE/JSE GOVI Index",
    index_rules = "Tracks SA government nominal bonds — those in which the Reserve Bank obliges primary dealers to make markets. Weighted by outstanding market value. Higher duration bonds carry more interest rate risk but offer higher yield. Reviewed monthly.",
    rebalancing = "Monthly",
    top_holdings = list(`R186 (2026)`=18.9,`R2030 (2030)`=16.5,`R2032 (2032)`=14.2,
                        `R2035 (2035)`=12.8,`R2037 (2037)`=11.6,`R2040 (2040)`=10.4,
                        `R2044 (2044)`=8.7,`R2048 (2048)`=6.9,`SA Govt Bond`=0.0,
                        `SA Govt Bond`=0.0)
  ),

  # ── SA PREFERENCE SHARES ────────────────────────────────────────────────────

  list(
    ticker = "PREFTX", yf_ticker = "PREFTX.JO",
    name = "CoreShares PrefTrax ETF", provider = "10X (CoreShares)",
    category = "SA Preference Share", ter = 0.0035, dividend = "Pays",
    index = "FTSE/JSE Preference Share Index",
    index_rules = "Tracks all JSE-listed preference shares. Preference shares pay fixed or variable dividends before ordinary shareholders and rank above equity in a wind-up. High income, relatively low capital volatility, but sensitive to interest rate changes. Reviewed quarterly.",
    rebalancing = "Quarterly",
    top_holdings = list(`Absa Pref`=22.4,`Standard Bank Pref`=18.9,`FirstRand Pref`=17.1,
                        `Nedbank Pref`=14.8,`Investec Pref`=10.5,`Growthpoint Pref`=8.2,
                        `Redefine Pref`=5.4,`Hyprop Pref`=2.7)
  ),

  # ── GLOBAL EQUITY ───────────────────────────────────────────────────────────

  list(
    ticker = "STX500", yf_ticker = "STX500.JO",
    name = "Satrix S&P 500 Feeder ETF", provider = "Satrix",
    category = "Global Equity", ter = 0.0035, dividend = "Reinvests",
    index = "S&P 500 Index",
    index_rules = "Feeder into Vanguard S&P 500 UCITS ETF. The S&P 500 covers 500 large US companies reviewed quarterly by the S&P 500 Index Committee. Minimum market cap, liquidity, float and consecutive positive earnings requirements apply.",
    rebalancing = "Quarterly",
    top_holdings = list(Apple=7.1,Microsoft=6.4,Nvidia=6.0,Amazon=3.9,Meta=2.8,
                        `Alphabet A`=2.1,`Alphabet C`=1.8,`Berkshire Hathaway`=1.7,
                        `Eli Lilly`=1.6,Broadcom=1.5)
  ),

  list(
    ticker = "SYG500", yf_ticker = "SYG500.JO",
    name = "Sygnia Itrix S&P 500 ETF", provider = "Sygnia",
    category = "Global Equity", ter = 0.0015, dividend = "Reinvests",
    index = "S&P 500 Index",
    index_rules = "Tracks the S&P 500 via iShares S&P 500 UCITS ETF. The lowest-cost S&P 500 product on the Easy Equities TFSA at 0.15% TER. Identical benchmark and holdings to STX500 and ETF500.",
    rebalancing = "Quarterly",
    top_holdings = list(Apple=7.1,Microsoft=6.4,Nvidia=6.0,Amazon=3.9,Meta=2.8,
                        `Alphabet A`=2.1,`Alphabet C`=1.8,`Berkshire Hathaway`=1.7,
                        `Eli Lilly`=1.6,Broadcom=1.5)
  ),

  list(
    ticker = "ETF500", yf_ticker = "ETF500.JO",
    name = "1nvest S&P 500 Feeder ETF", provider = "1nvest",
    category = "Global Equity", ter = 0.0020, dividend = "Reinvests",
    index = "S&P 500 Index",
    index_rules = "Feeder into iShares Core S&P 500 UCITS ETF. Managed by Standard Bank's 1nvest. Same benchmark as STX500 and SYG500 at a cost between the two.",
    rebalancing = "Quarterly",
    top_holdings = list(Apple=7.1,Microsoft=6.4,Nvidia=6.0,Amazon=3.9,Meta=2.8,
                        `Alphabet A`=2.1,`Alphabet C`=1.8,`Berkshire Hathaway`=1.7,
                        `Eli Lilly`=1.6,Broadcom=1.5)
  ),

  list(
    ticker = "CSP500", yf_ticker = "CSP500.JO",
    name = "10X S&P 500 ETF", provider = "10X (CoreShares)",
    category = "Global Equity", ter = 0.0037, dividend = "Reinvests",
    index = "S&P 500 Index",
    index_rules = "Feeder into iShares S&P 500 UCITS ETF. Managed by 10X (formerly CoreShares). Same benchmark as the other S&P 500 ETFs — choice between them is primarily a TER decision.",
    rebalancing = "Quarterly",
    top_holdings = list(Apple=7.1,Microsoft=6.4,Nvidia=6.0,Amazon=3.9,Meta=2.8,
                        `Alphabet A`=2.1,`Alphabet C`=1.8,`Berkshire Hathaway`=1.7,
                        `Eli Lilly`=1.6,Broadcom=1.5)
  ),

  list(
    ticker = "ETF5IT", yf_ticker = "ETF5IT.JO",
    name = "1nvest S&P 500 Info Tech Feeder ETF", provider = "1nvest",
    category = "Global Equity", ter = 0.0025, dividend = "Reinvests",
    index = "S&P 500 Information Technology Sector Index",
    index_rules = "Tracks only the Information Technology sector of the S&P 500 — approximately 70 companies. Much more concentrated than a full S&P 500 product. Dominated by mega-cap tech. Reviewed quarterly by S&P Dow Jones Indices.",
    rebalancing = "Quarterly",
    top_holdings = list(Apple=21.5,Microsoft=19.8,Nvidia=18.2,Broadcom=5.4,
                        `TSMC ADR`=3.1,Salesforce=2.8,AMD=2.5,Qualcomm=2.2,
                        Oracle=2.1,`Texas Instruments`=1.8)
  ),

  list(
    ticker = "STXNDQ", yf_ticker = "STXNDQ.JO",
    name = "Satrix Nasdaq 100 Feeder ETF", provider = "Satrix",
    category = "Global Equity", ter = 0.0048, dividend = "Reinvests",
    index = "Nasdaq-100 Index",
    index_rules = "Feeder into Invesco QQQ / Invesco Nasdaq-100 UCITS ETF. The Nasdaq-100 includes the 100 largest non-financial companies listed on Nasdaq, weighted by modified market cap. Heavily tech-oriented. Reviewed annually in December.",
    rebalancing = "Annual (December)",
    top_holdings = list(Apple=9.1,Microsoft=8.5,Nvidia=8.2,Amazon=5.3,Meta=4.8,
                        `Alphabet A`=3.4,Broadcom=3.1,Tesla=2.9,Costco=2.6,Netflix=2.3)
  ),

  list(
    ticker = "STXWDM", yf_ticker = "STXWDM.JO",
    name = "Satrix MSCI World Feeder ETF", provider = "Satrix",
    category = "Global Equity", ter = 0.0035, dividend = "Reinvests",
    index = "MSCI World Index",
    index_rules = "Feeder into Vanguard MSCI World UCITS ETF. MSCI World covers ~1,500 large/mid cap stocks across 23 developed markets. US ≈ 70%, Japan ≈ 6%, UK ≈ 4%, France ≈ 3.5%. No emerging market exposure. Reviewed quarterly, reconstituted semi-annually.",
    rebalancing = "Quarterly",
    top_holdings = list(Apple=4.9,Microsoft=4.4,Nvidia=4.1,Amazon=2.7,Meta=1.9,
                        `Alphabet A`=1.5,`Berkshire Hathaway`=1.2,`Eli Lilly`=1.1,
                        Broadcom=1.0,LVMH=0.9)
  ),

  list(
    ticker = "SYGWD", yf_ticker = "SYGWD.JO",
    name = "Sygnia Itrix MSCI World ETF", provider = "Sygnia",
    category = "Global Equity", ter = 0.0020, dividend = "Reinvests",
    index = "MSCI World Index",
    index_rules = "Replicates the MSCI World Index via iShares Core MSCI World UCITS ETF. Same benchmark as STXWDM but at a lower TER (0.20% vs 0.35%). Covers ~1,500 developed market large/mid cap stocks across 23 countries.",
    rebalancing = "Quarterly",
    top_holdings = list(Apple=4.9,Microsoft=4.4,Nvidia=4.1,Amazon=2.7,Meta=1.9,
                        `Alphabet A`=1.5,`Berkshire Hathaway`=1.2,`Eli Lilly`=1.1,
                        Broadcom=1.0,LVMH=0.9)
  ),

  list(
    ticker = "CTWRL", yf_ticker = "CTWRL.JO",
    name = "10X Total World ETF", provider = "10X (CoreShares)",
    category = "Global Equity", ter = 0.0025, dividend = "Reinvests",
    index = "FTSE Global All Cap Index",
    index_rules = "Feeder into Vanguard Total World Stock UCITS ETF. Tracks the FTSE Global All Cap Index — 9,500+ stocks across 49 countries including both developed AND emerging markets, plus small/mid caps. The broadest single-fund global equity exposure on the Easy Equities TFSA.",
    rebalancing = "Quarterly",
    top_holdings = list(Apple=4.2,Microsoft=3.8,Nvidia=3.5,Amazon=2.3,Meta=1.6,
                        `Alphabet A`=1.3,`Taiwan Semiconductor`=1.0,`Berkshire Hathaway`=1.0,
                        `Eli Lilly`=0.9,Samsung=0.8)
  ),

  list(
    ticker = "STXEMG", yf_ticker = "STXEMG.JO",
    name = "Satrix MSCI Emerging Markets Feeder ETF", provider = "Satrix",
    category = "EM Equity", ter = 0.0040, dividend = "Reinvests",
    index = "MSCI Emerging Markets Index",
    index_rules = "Feeder into Vanguard MSCI Emerging Markets UCITS ETF. Covers ~1,400 large/mid cap stocks in 24 EM countries. Top country weights: China ~30%, Taiwan ~15%, India ~15%, South Korea ~12%. Reviewed quarterly, reconstituted semi-annually in May and November.",
    rebalancing = "Quarterly",
    top_holdings = list(`Taiwan Semiconductor`=8.5,`Samsung Electronics`=4.1,Tencent=3.8,
                        Alibaba=2.9,Meituan=2.4,`HDFC Bank`=2.1,`Reliance Industries`=2.0,
                        Infosys=1.8,`SK Hynix`=1.6,`ICICI Bank`=1.5)
  ),

  list(
    ticker = "ASHGEQ", yf_ticker = "ASHGEQ.JO",
    name = "Ashburton Global 1200 ETF", provider = "Ashburton (FNB)",
    category = "Global Equity", ter = 0.0075, dividend = "Reinvests",
    index = "S&P Global 1200 Index",
    index_rules = "Tracks the S&P Global 1200 — a composite of 7 regional indices covering 31 countries and ~1,200 leading global companies. Combines S&P 500, S&P Europe 350, S&P TOPIX 150 (Japan), S&P/ASX 50 (Australia), S&P Asia 50, S&P Latin America 40 and S&P Canada 60.",
    rebalancing = "Quarterly",
    top_holdings = list(Apple=4.5,Microsoft=4.0,Nvidia=3.8,Amazon=2.6,Meta=1.8,
                        `Alphabet A`=1.4,`Berkshire Hathaway`=1.1,ASML=0.9,
                        `Eli Lilly`=0.9,`Novo Nordisk`=0.8)
  ),

  list(
    ticker = "SYGJP", yf_ticker = "SYGJP.JO",
    name = "Sygnia Itrix MSCI Japan ETF", provider = "Sygnia",
    category = "Global Equity", ter = 0.0020, dividend = "Reinvests",
    index = "MSCI Japan Index",
    index_rules = "Tracks the MSCI Japan Index via iShares Core MSCI Japan IMI UCITS ETF. Covers ~1,200 Japanese companies including small/mid caps. Japan is the world's third-largest equity market. Provides pure Japan exposure isolated from the broader MSCI World.",
    rebalancing = "Quarterly",
    top_holdings = list(`Toyota Motor`=4.8,Sony=3.6,Keyence=3.1,`Recruit Holdings`=2.8,
                        `Shin-Etsu Chemical`=2.5,`Tokyo Electron`=2.3,KDDI=2.1,`Honda Motor`=1.9,
                        `SoftBank Group`=1.8,`Mitsubishi UFJ`=1.6)
  ),

  list(
    ticker = "SYGEU", yf_ticker = "SYGEU.JO",
    name = "Sygnia Itrix MSCI Europe ETF", provider = "Sygnia",
    category = "Global Equity", ter = 0.0020, dividend = "Reinvests",
    index = "MSCI Europe Index",
    index_rules = "Tracks the MSCI Europe Index via iShares Core MSCI Europe UCITS ETF. Covers ~430 large/mid cap companies across 15 European developed markets. Top countries: UK, France, Germany, Switzerland. Reviewed quarterly.",
    rebalancing = "Quarterly",
    top_holdings = list(ASML=4.2,Nestlé=3.8,LVMH=3.5,`Novo Nordisk`=3.3,SAP=2.9,
                        Roche=2.6,AstraZeneca=2.4,Shell=2.2,Hermès=2.0,Siemens=1.9)
  ),

  list(
    ticker = "SYGUK", yf_ticker = "SYGUK.JO",
    name = "Sygnia Itrix MSCI UK ETF", provider = "Sygnia",
    category = "Global Equity", ter = 0.0020, dividend = "Reinvests",
    index = "MSCI United Kingdom Index",
    index_rules = "Tracks the MSCI UK Index via iShares Core MSCI UK IMI UCITS ETF. Covers ~250 UK-listed companies across all sizes. Heavily weighted to financials, energy and consumer staples. Reviewed quarterly.",
    rebalancing = "Quarterly",
    top_holdings = list(AstraZeneca=9.8,Shell=8.5,HSBC=7.2,Unilever=6.4,BP=5.1,
                        `Rio Tinto`=4.8,GSK=4.4,Diageo=4.1,`BHP (UK listing)`=3.8,RELX=3.4)
  ),

  # ── GLOBAL PROPERTY ─────────────────────────────────────────────────────────

  list(
    ticker = "GLPROP", yf_ticker = "GLPROP.JO",
    name = "10X S&P Global Property ETF", provider = "10X (CoreShares)",
    category = "Global Property", ter = 0.0051, dividend = "Pays",
    index = "S&P Global Property 40 Index",
    index_rules = "Tracks 40 of the largest global real estate companies and REITs across developed markets, selected by float-adjusted market cap. Provides exposure to US, European, Asian and Australian property markets. Reviewed quarterly.",
    rebalancing = "Quarterly",
    top_holdings = list(Prologis=8.2,`American Tower`=7.4,Equinix=6.8,`Simon Property`=5.9,
                        `Crown Castle`=5.1,`Public Storage`=4.8,`Realty Income`=4.5,
                        Welltower=4.2,AvalonBay=3.8,`Digital Realty`=3.5)
  ),

  list(
    ticker = "ETFGPR", yf_ticker = "ETFGPR.JO",
    name = "1nvest Global REIT Index Feeder ETF", provider = "1nvest",
    category = "Global Property", ter = 0.0025, dividend = "Pays",
    index = "FTSE EPRA/NAREIT Global REIT Index",
    index_rules = "Tracks the FTSE EPRA/NAREIT Global REIT Index via iShares Global REIT ETF. Covers ~300 REITs across 25 countries. More diversified than the S&P Global Property 40 and uses the widely-adopted NAREIT classification methodology.",
    rebalancing = "Quarterly",
    top_holdings = list(Prologis=8.5,`American Tower`=6.9,Equinix=6.4,`Simon Property`=5.5,
                        `Public Storage`=4.9,`Crown Castle`=4.7,`Realty Income`=4.2,
                        Welltower=3.9,Ventas=3.6,`Alexandria RE`=3.2)
  ),

  # ── GLOBAL INCOME / DIVIDEND ─────────────────────────────────────────────────

  list(
    ticker = "GLODIV", yf_ticker = "GLODIV.JO",
    name = "10X S&P Global Dividend ETF", provider = "10X (CoreShares)",
    category = "Global Income", ter = 0.0054, dividend = "Pays",
    index = "S&P Global Dividend Aristocrats Index",
    index_rules = "Selects global stocks with sustainable high dividend yields. Constituents must have paid non-decreasing dividends for at least 7 consecutive years and pass liquidity screens. Weighted by dividend yield. Reviewed annually. Provides international income exposure.",
    rebalancing = "Annual",
    top_holdings = list(`Fortescue Metals`=4.8,`Altria Group`=4.4,`T. Rowe Price`=3.9,
                        `Realty Income`=3.7,Enbridge=3.5,AT&T=3.1,`BCE Inc`=2.9,
                        `Kinder Morgan`=2.7,`Eversource Energy`=2.5,`Leggett & Platt`=2.3)
  ),

  # ── GLOBAL BONDS ─────────────────────────────────────────────────────────────

  list(
    ticker = "ETFUSG", yf_ticker = "ETFUSG.JO",
    name = "1nvest ICE US Treasury Short Bond Feeder ETF", provider = "1nvest",
    category = "Global Bond", ter = 0.0020, dividend = "Pays",
    index = "ICE US Treasury Short Bond Index",
    index_rules = "Tracks short-dated US Treasury bonds (1–3 year maturities) via iShares Short Treasury Bond ETF. Low-risk USD-denominated income instrument. ZAR-denominated returns include currency (ZAR/USD) effects. Reviewed monthly.",
    rebalancing = "Monthly",
    top_holdings = list(`US Treasury 2Y`=19.5,`US Treasury 1Y`=18.2,`US Treasury 18M`=16.8,
                        `US Treasury 3Y`=15.1,`US T-Bill 6M`=12.6,`US T-Bill 3M`=10.9,
                        `US T-Bill 1M`=6.9)
  ),

  # ── COMMODITIES ──────────────────────────────────────────────────────────────

  list(
    ticker = "ETFGLD", yf_ticker = "ETFGLD.JO",
    name = "1nvest Gold ETF", provider = "1nvest",
    category = "Commodity", ter = 0.0040, dividend = "None",
    index = "ZAR Gold Spot Price",
    index_rules = "Physically-backed gold ETF. Each debenture represents 1/100th of a fine troy ounce of gold stored in SA Reserve Bank vaults. Tracks the ZAR gold price directly. No index — single physical asset. Provides a rand hedge and inflation protection.",
    rebalancing = "N/A",
    top_holdings = list(`Physical Gold (100%)` = 100.0)
  ),

  list(
    ticker = "NGPLT", yf_ticker = "NGPLT.JO",
    name = "NewPlat ETF", provider = "Absa (NewGold)",
    category = "Commodity", ter = 0.0040, dividend = "None",
    index = "ZAR Platinum Spot Price",
    index_rules = "Physically-backed platinum ETF issued by NewGold Owner Trust (an Absa subsidiary). Each debenture represents 1/100th of a fine troy ounce of physical platinum held in SA vaults. Tracks the ZAR platinum spot price. Highly sensitive to autocatalyst demand (ICE vehicles).",
    rebalancing = "N/A",
    top_holdings = list(`Physical Platinum (100%)` = 100.0)
  ),

  list(
    ticker = "ETFPLT", yf_ticker = "ETFPLT.JO",
    name = "1nvest Platinum ETF", provider = "1nvest",
    category = "Commodity", ter = 0.0040, dividend = "None",
    index = "ZAR Platinum Spot Price",
    index_rules = "Physically-backed platinum ETF managed by Standard Bank's 1nvest. Identical structure to NGPLT — each debenture backed by 1/100th of a fine troy ounce held by the SA Reserve Bank. Same spot price exposure at an identical cost.",
    rebalancing = "N/A",
    top_holdings = list(`Physical Platinum (100%)` = 100.0)
  ),

  list(
    ticker = "ETFPLD", yf_ticker = "ETFPLD.JO",
    name = "1nvest Palladium ETF", provider = "1nvest",
    category = "Commodity", ter = 0.0040, dividend = "None",
    index = "ZAR Palladium Spot Price",
    index_rules = "Physically-backed palladium ETF. Each debenture backed by 1/100th of a fine troy ounce of physical palladium. Palladium supply is heavily concentrated in Russia and South Africa. Extremely high price volatility driven by catalytic converter demand and geopolitical supply risks.",
    rebalancing = "N/A",
    top_holdings = list(`Physical Palladium (100%)` = 100.0)
  ),

  list(
    ticker = "ETFRHO", yf_ticker = "ETFRHO.JO",
    name = "1nvest Rhodium ETF", provider = "1nvest",
    category = "Commodity", ter = 0.0040, dividend = "None",
    index = "ZAR Rhodium Spot Price",
    index_rules = "Physically-backed rhodium ETF — one of the few in the world. Each debenture represents 1/100th of a fine troy ounce of physical rhodium. Rhodium is the rarest PGM, used in three-way catalytic converters. Notorious for extreme boom-bust price cycles and very low liquidity.",
    rebalancing = "N/A",
    top_holdings = list(`Physical Rhodium (100%)` = 100.0)
  ),

  # ── MULTI-ASSET ───────────────────────────────────────────────────────────────

  list(
    ticker = "MAPPSG", yf_ticker = "MAPPSG.JO",
    name = "Satrix MAPPS Growth ETF", provider = "Satrix",
    category = "Multi-Asset", ter = 0.0025, dividend = "Pays",
    index = "MAPPS Growth Index",
    index_rules = "Blended multi-asset ETF with a growth-biased allocation: SA Equity 75%, Nominal Bonds 10%, Inflation-Linked Bonds 10%, Cash 5%. Previously managed by NewFunds (Absa), transferred to Satrix in March 2023. Targets long-term capital growth with some inflation protection.",
    rebalancing = "Monthly",
    top_holdings = list(`SA Equity Basket`=75.0,`SA Nominal Bonds`=10.0,
                        `SA Inflation Bonds`=10.0,`Cash / Money Market`=5.0)
  ),

  list(
    ticker = "MAPPSP", yf_ticker = "MAPPSP.JO",
    name = "Satrix MAPPS Protect ETF", provider = "Satrix",
    category = "Multi-Asset", ter = 0.0025, dividend = "Pays",
    index = "MAPPS Protect Index",
    index_rules = "Conservative multi-asset ETF prioritising capital preservation: SA Equity 40%, Inflation-Linked Bonds 35%, Nominal Bonds 15%, Cash 10%. The heavy ILB weight provides real return protection. Transferred from NewFunds to Satrix in March 2023.",
    rebalancing = "Monthly",
    top_holdings = list(`SA Equity Basket`=40.0,`SA Inflation Bonds`=35.0,
                        `SA Nominal Bonds`=15.0,`Cash / Money Market`=10.0)
  )

)

# ── Convert to tidy data frame ────────────────────────────────────────────────

etf_df <- map_dfr(etf_catalogue, function(e) {
  tibble(
    ticker      = e$ticker,   yf_ticker   = e$yf_ticker,
    name        = e$name,     provider    = e$provider,
    category    = e$category, ter         = e$ter,
    dividend    = e$dividend, index       = e$index,
    index_rules = e$index_rules, rebalancing = e$rebalancing
  )
})

message(glue("Pipeline start — {nrow(etf_df)} ETFs catalogued"))
message(glue("Date range   : {START_DATE} → {END_DATE}"))
message(glue("Output dir   : {OUTPUT_DIR}\n"))

# ── Analytics Helpers ─────────────────────────────────────────────────────────

fetch_prices <- function(yf_ticker, name) {
  tryCatch({
    p <- tq_get(yf_ticker, from = START_DATE, to = END_DATE)
    if (is.null(p) || nrow(p) < 50) stop("insufficient data")
    p
  }, error = function(e) { warning(glue("  ✗ {name}: {e$message}")); NULL })
}

compute_metrics <- function(prices) {
  ret <- prices |> arrange(date) |>
    mutate(r = adjusted / lag(adjusted) - 1) |> filter(!is.na(r))
  n <- nrow(ret); yrs <- n / 252
  mu <- mean(ret$r); sig <- sd(ret$r)
  total  <- prod(1 + ret$r) - 1
  cagr   <- (1 + total)^(1/yrs) - 1
  vol    <- sig * sqrt(252)
  sharpe <- (mu - DAILY_RF) / sig * sqrt(252)
  dd_sd  <- sd(ret$r[ret$r < DAILY_RF])
  sortino <- if (is.finite(dd_sd) && dd_sd > 0) (mu - DAILY_RF) / dd_sd * sqrt(252) else NA_real_
  cum <- cumprod(1 + ret$r); peak <- cummax(cum); dd_vec <- cum/peak - 1
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
  prices |> arrange(date) |> mutate(ym = floor_date(date,"month")) |>
    group_by(ym) |> summarise(r = last(adjusted)/first(adjusted)-1, .groups="drop") |>
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
    mutate(across(where(is.numeric), ~round(.x,6)))
}

compute_drawdown_series <- function(prices) {
  prices |> arrange(date) |>
    mutate(cum=adjusted/first(adjusted), peak=cummax(cum), drawdown=round(cum/peak-1,6)) |>
    mutate(week=floor_date(date,"week")) |> group_by(week) |>
    slice_tail(n=1) |> ungroup() |> select(date, drawdown)
}

build_price_series <- function(prices) {
  prices |> arrange(date) |> mutate(week=floor_date(date,"week")) |>
    group_by(week) |>
    summarise(date=last(date), open=first(open), high=max(high), low=min(low),
              close=last(close), volume=sum(volume), adjusted=last(adjusted), .groups="drop") |>
    mutate(daily_return=round(adjusted/lag(adjusted)-1,6),
           cum_return=round(adjusted/first(adjusted)-1,6)) |>
    select(-week) |>
    mutate(across(c(open,high,low,close,adjusted), ~round(.x,4)))
}

# ── Main Loop ─────────────────────────────────────────────────────────────────

summary_rows <- list()
processed    <- character(0)

for (i in seq_len(nrow(etf_df))) {
  row   <- etf_df[i, ]
  meta  <- etf_catalogue[[i]]
  ticker <- row$ticker

  message(glue("[{i}/{nrow(etf_df)}] {ticker}  —  {row$name}"))

  prices <- fetch_prices(row$yf_ticker, row$name)
  if (is.null(prices)) next

  metrics  <- compute_metrics(prices)
  monthly  <- compute_monthly_returns(prices)
  rolling  <- compute_rolling_vol(prices)
  drawdown <- compute_drawdown_series(prices)
  price_ts <- build_price_series(prices)

  holdings_arr <- imap(meta$top_holdings, ~list(name=.y, weight=.x)) |>
    keep(~nchar(.x$name) > 0 && .x$weight > 0) |> unname()

  payload <- list(
    ticker=ticker, name=row$name, provider=row$provider,
    category=row$category, ter=row$ter, dividend=row$dividend,
    index=row$index, index_rules=row$index_rules, rebalancing=row$rebalancing,
    top_holdings=holdings_arr, metrics=metrics, prices=price_ts,
    monthly_returns=monthly, rolling_vol=rolling, drawdown=drawdown
  )

  write_json(payload, file.path(OUTPUT_DIR, glue("{ticker}.json")),
             auto_unbox=TRUE, digits=6, pretty=FALSE)
  message(glue("  ✓ saved\n"))

  processed <- c(processed, ticker)
  summary_rows[[ticker]] <- tibble(
    ticker=ticker, name=row$name, provider=row$provider,
    category=row$category, ter=row$ter, dividend=row$dividend,
    ytd_return=metrics$ytd_return, return_1y=metrics$return_1y,
    return_3y=metrics$return_3y, cagr=metrics$cagr,
    ann_vol=metrics$ann_vol, sharpe=metrics$sharpe,
    sortino=metrics$sortino, calmar=metrics$calmar,
    max_drawdown=metrics$max_drawdown, var_95=metrics$var_95,
    n_trading_days=metrics$n_trading_days
  )
}

summary_df <- bind_rows(summary_rows)
write_json(summary_df, file.path(OUTPUT_DIR,"summary.json"), auto_unbox=TRUE, digits=6)
write_json(etf_df |> filter(ticker %in% processed),
           file.path(OUTPUT_DIR,"etf_list.json"), auto_unbox=TRUE, pretty=TRUE)

message(glue(
  "\n── Complete ─────────────────────────────────────────────────────\n",
  "  Processed : {length(processed)} / {nrow(etf_df)}\n",
  "  Output    : {OUTPUT_DIR}\n",
  "  Timestamp : {format(Sys.time(),'%Y-%m-%d %H:%M:%S')}\n",
  "─────────────────────────────────────────────────────────────────"
))
