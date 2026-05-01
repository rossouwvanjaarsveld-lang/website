# =============================================================================
# Easy Equities TFSA ETF  —  Data Pipeline
# =============================================================================
# DATA SOURCES
#   - ETF list:      posts/ee-etf-tracker/data/easy_equities_tfsa_etfs.csv
#                    (Provider, ETF Name, Ticker)
#   - Live data:     justonelap.com/{ticker}/  (benchmark, classification,
#                    top holdings, TIC/TER, distribution, description)
#                    Just One Lap aggregates Minimum Disclosure Documents
#                    that all JSE ETF issuers are legally required to publish.
#   - Prices:        Yahoo Finance (primary), Stooq (fallback)
#
# OUTPUTS
#   posts/ee-etf-tracker/data/
#     etf_list.json        — full catalogue with EE display names + aliases
#     summary.json         — one row per ETF for the comparison table
#     <TICKER>.json        — full payload per ETF
#
# DEPENDENCIES
#   install.packages(c("tidyverse","tidyquant","quantmod","lubridate",
#                      "slider","jsonlite","glue","here","rvest","httr","stringi"))
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
  library(stringi)
})

# ── Config ────────────────────────────────────────────────────────────────────

POST_DIR     <- here("posts", "ee-etf-tracker")
DATA_DIR     <- file.path(POST_DIR, "data")
CSV_PATH     <- file.path(DATA_DIR, "easy_equities_tfsa_etfs.csv")
START_DATE   <- as.Date("2019-01-01")
END_DATE     <- Sys.Date()
RISK_FREE    <- 0.0825
DAILY_RF     <- RISK_FREE / 252
MIN_ROWS     <- 20

UA <- paste0("Mozilla/5.0 (Windows NT 10.0; Win64; x64) ",
             "AppleWebKit/537.36 (KHTML, like Gecko) ",
             "Chrome/121.0.0.0 Safari/537.36")

dir.create(DATA_DIR, recursive = TRUE, showWarnings = FALSE)

# ── Read the master CSV (the source of truth for what's available) ───────────

if (!file.exists(CSV_PATH)) {
  stop(glue("CSV not found at {CSV_PATH}\nPlace easy_equities_tfsa_etfs.csv there before running."))
}

# ── Ticker corrections ────────────────────────────────────────────────────────
# Yahoo Finance and JSE codes don't always match. This map overrides the
# default `{ticker}.JO` for known mismatches. Verified manually — see notes.
# Values of NA mean "no Yahoo Finance equivalent exists" (typically AMETFs
# too new for Yahoo's coverage).

YF_TICKER_OVERRIDES <- c(
  # ── Wrong Yahoo symbol (JSE code differs from Yahoo's symbol) ───────────────
  STXVLE   = "STXVEQ.JO",  # Satrix Value Equity
  STXINF   = "STXIFR.JO",  # Satrix Global Infrastructure Feeder
  STXSCF   = "STXCTY.JO",  # Satrix Smart City Infrastructure
  STXWESG  = "STXESG.JO",  # Satrix MSCI World ESG Enhanced
  STXINDI  = "STXNDA.JO",  # Satrix MSCI India Feeder
  FNB40    = "FNBT40.JO",  # FNB Top 40
  FNBGLO   = "FNBEQF.JO",  # FNB Global 1200 (was ASHGEQ → ASHEQF → FNBEQF)
  REITGP   = "RWAGP.JO",   # Reitway Global Property Actively Managed Prescient
  SYGEMF50 = "SYGEMF.JO",  # Sygnia MSCI EM 50 (CSV had spurious "50" suffix)
  SYGHLT   = "SYGH.JO",    # Sygnia Itrix Solactive Healthcare 150
  
  # ── Renamed / amalgamated since the CSV was published ──────────────────────
  STXSWX   = "STXSAI.JO",  # Renamed to Satrix SAI on 16 Jan 2026
  STXMAP   = "STXGLB.JO",  # Amalgamated with STXGLB on 30 Jun 2025
  STXMPF   = "STXGLB.JO"   # Old code → STXMAP → STXGLB
  # Note: STXHLT and STXTRC use their default {ticker}.JO — both exist on Yahoo.
  # AMETFs like APACXJ, INCOME, DIVTRX, SMART also exist as {ticker}.JO so we
  # let the default through. Funds that genuinely have no Yahoo coverage
  # (PIPF, PMXINC, ETFBAF, EASYAI, EASYB, EASYBAL, EASYGE) will simply fail
  # the price fetch gracefully and pick up returns from EasyCompare instead.
)

# JOL slug overrides (their URL is /{ticker}/ but a few use different slugs)
JOL_SLUG_OVERRIDES <- c(
  STXSWX   = "stxsai",  # JOL follows the new Satrix SAI page
  STXMAP   = "stxglb",
  STXMPF   = "stxglb",
  STXVLE   = "stxveq",
  STXINF   = "stxifr",
  STXSCF   = "stxcty",
  STXWESG  = "stxesg",
  STXINDI  = "stxnda",  # Satrix MSCI India
  FNB40    = "fnbt40",
  FNBGLO   = "fnbeqf",  # FNB Global 1200
  REITGP   = "rwagp",
  SYGEMF50 = "sygemf",  # Sygnia EM 50
  SYGHLT   = "sygh"     # Sygnia Healthcare
)

# EasyETFs instrument page slugs — for AMETFs published by EasyAssetManagement.
# Found by inspecting etfs.easyequities.co.za. Empty string = no instrument page.
EASYETF_PAGE_SLUGS <- c(
  EASYAI  = "easyai",
  EASYB   = "easybf",
  EASYBAL = "cartbl",
  EASYGE  = "easyge"
)

etf_csv <- read_csv(CSV_PATH, show_col_types = FALSE) |>
  rename(provider = Provider, name = `ETF Name`, ticker = Ticker) |>
  mutate(
    ticker   = str_trim(ticker),
    name     = str_trim(name),
    provider = str_trim(provider),
    # Apply ticker overrides — falls back to {ticker}.JO if no override
    yf = map_chr(ticker, function(t) {
      if (t %in% names(YF_TICKER_OVERRIDES)) {
        v <- YF_TICKER_OVERRIDES[[t]]
        if (is.na(v)) return(NA_character_) else return(v)
      }
      paste0(t, ".JO")
    }),
    # JOL slug override
    jol_slug = map_chr(ticker, function(t) {
      if (t %in% names(JOL_SLUG_OVERRIDES)) JOL_SLUG_OVERRIDES[[t]] else tolower(t)
    })
  ) |>
  distinct(ticker, .keep_all = TRUE) |>
  arrange(provider, ticker)

message(glue("Loaded {nrow(etf_csv)} ETFs from CSV"))
message(glue("  · {sum(!is.na(etf_csv$yf))} have Yahoo Finance tickers configured"))
message(glue("  · {sum(is.na(etf_csv$yf))} flagged as no-Yahoo-data (AMETFs / too new)"))

# =============================================================================
# EASY EQUITIES DISPLAY NAME ALIASES
# =============================================================================
# Easy Equities often abbreviates or restructures fund names compared to the
# manager's official / EasyCompare name. Where we know an alternative, we add
# it here so search works either way. Update as new aliases are discovered.
# =============================================================================

EE_ALIASES <- list(
  STX40    = c("Satrix Top 40 ETF", "Satrix 40", "Top 40 Satrix"),
  STXSWX   = c("Satrix SWIX 40", "Satrix Swix Top 40"),
  STXCAP   = c("Satrix Capped All Share", "Satrix CAPI"),
  STXDIV   = c("Satrix Dividend Plus", "Satrix Divi", "Satrix Dividend"),
  STXFIN   = c("Satrix Financials", "Satrix FINI"),
  STXIND   = c("Satrix Industrials", "Satrix INDI"),
  STXRES   = c("Satrix Resources", "Satrix RESI"),
  STXMMT   = c("Satrix Momentum"),
  STXLVL   = c("Satrix Low Vol", "Satrix Lo Vol"),
  STXQUA   = c("Satrix Quality"),
  STXRAF   = c("Satrix RAFI", "Satrix Rafi 40"),
  STXPRO   = c("Satrix SA Property", "Satrix Property"),
  STXILB   = c("Satrix Inflation Linked Bond", "Satrix ILBI"),
  STXGOV   = c("Satrix GOVI", "Satrix Government Bond"),
  STX500   = c("Satrix S&P500", "Satrix 500"),
  STXNDQ   = c("Satrix Nasdaq", "Satrix Nasdaq 100 Feeder"),
  STXWDM   = c("Satrix MSCI World", "Satrix World"),
  STXEMG   = c("Satrix Emerging Markets", "Satrix MSCI EM"),
  STXCHN   = c("Satrix MSCI China"),
  STXINDI  = c("Satrix India", "Satrix MSCI India"),
  STXSCF   = c("Satrix Smart City"),
  STXHLT   = c("Satrix Healthcare"),
  STXSHA   = c("Satrix Shariah Top 40", "Satrix Shari'ah", "Satrix Shariah"),
  STXVLE   = c("Satrix Value"),
  STXID    = c("Satrix Inclusion Diversity"),
  STXEME   = c("Satrix EM ESG"),
  STXWESG  = c("Satrix World ESG"),
  STXNAM   = c("Satrix Namibia Bond"),
  STXMAP   = c("Satrix MAPPS Growth"),
  STXMPF   = c("Satrix MAPPS Protect"),
  STXTRC   = c("Satrix TRACI"),
  STXGBD   = c("Satrix Global Bond"),
  STXINF   = c("Satrix Global Infrastructure"),
  
  CSP500   = c("10X S&P500", "CoreShares S&P 500"),
  CTOP50   = c("10X Top 50", "CoreShares Top 50"),
  GLPROP   = c("10X Global Property", "CoreShares Global Property"),
  GLODIV   = c("10X Global Dividend", "CoreShares Global Dividend"),
  GLOBAL   = c("10X Total World", "CoreShares Total World"),
  CSPROP   = c("10X SA Property", "CoreShares SA Property"),
  CSGOVI   = c("10X Govi", "CoreShares Govi"),
  CSYSB    = c("10X Yield Selected", "10X PrefTrax Successor", "PrefTrax replaced"),
  DIVTRX   = c("10X DivTrax", "CoreShares DivTrax"),
  SMART    = c("10X Multi Factor", "10X Smart Beta", "Scientific Beta"),
  INCOME   = c("10X Income", "Active Income"),
  APACXJ   = c("10X All Asia"),
  
  ETFT40   = c("1nvest Top 40", "Stanlib Top 40"),
  ETF500   = c("1nvest S&P 500", "Stanlib S&P 500"),
  ETF5IT   = c("1nvest S&P 500 IT", "1nvest Tech"),
  ETFWLD   = c("1nvest MSCI World", "1nvest World"),
  ETFGRE   = c("1nvest Global REIT", "1nvest REIT"),
  ETFEMA   = c("1nvest EM Asia", "1nvest Asia"),
  ETFGGB   = c("1nvest Global Govt Bond"),
  ETFUSD   = c("1nvest US Treasury Short", "1nvest USD Bond"),
  ETFSRI   = c("1nvest SRI", "1nvest World Socially Responsible"),
  ETFBND   = c("1nvest SA Bond"),
  ETFSAP   = c("1nvest SA Property"),
  
  SYG500   = c("Sygnia S&P 500", "Sygnia 500"),
  SYGWD    = c("Sygnia MSCI World", "Sygnia World"),
  SYGUS    = c("Sygnia MSCI US", "Sygnia US"),
  SYGEU    = c("Sygnia Eurostoxx", "Sygnia Europe"),
  SYGUK    = c("Sygnia FTSE 100", "Sygnia UK"),
  SYGJP    = c("Sygnia Japan", "Sygnia MSCI Japan"),
  SYGCN    = c("Sygnia China", "Sygnia New China"),
  SYGT40   = c("Sygnia Top 40"),
  SYGP     = c("Sygnia Global Property"),
  SYGEMF50 = c("Sygnia EM 50", "Sygnia Emerging Markets 50"),
  SYGESG   = c("Sygnia ESG", "Sygnia 1200 ESG"),
  SYG4IR   = c("Sygnia 4IR", "Sygnia 4th Industrial"),
  SYGHLT   = c("Sygnia Healthcare", "Sygnia Solactive Healthcare"),
  
  FNB40    = c("FNB Top 40", "Ashburton Top 40"),
  FNBGLO   = c("FNB Global 1200", "Ashburton Global 1200"),
  FNBINF   = c("FNB Inflation"),
  FNBMID   = c("FNB MidCap"),
  FNBWGB   = c("FNB World Government Bond"),
  
  EASYAI   = c("EasyETFs AI"),
  EASYB    = c("EasyETFs Balanced"),
  EASYBAL  = c("Cartesian Balanced"),
  EASYGE   = c("EasyETFs Global Equity"),
  ETFBAF   = c("ETFSA Balanced"),
  PMXINC   = c("PortfolioMetrix Active Income"),
  PIPF     = c("Prescient Income Provider"),
  REITGP   = c("Reitway Global Property")
)

# Add aliases column to catalogue
etf_csv <- etf_csv |>
  rowwise() |>
  mutate(
    ee_aliases = list(EE_ALIASES[[ticker]] %||% character(0))
  ) |>
  ungroup()

# =============================================================================
# JUST ONE LAP SCRAPER  —  pulls live data from MDD-derived pages
# =============================================================================

safe_get <- function(url, timeout = 25) {
  tryCatch(
    GET(url, timeout(timeout),
        add_headers(`User-Agent` = UA,
                    `Accept` = "text/html,application/xhtml+xml,*/*;q=0.8",
                    `Accept-Language` = "en-US,en;q=0.9")),
    error = function(e) structure(list(error = e$message), class = "fetch_fail")
  )
}

#' Scrape the EasyCompare Finder page — a SINGLE page with performance
#' data for every TFSA-eligible ETF. We parse it once and cache the result
#' so we hit the network exactly one time per pipeline run.
#'
#' Returns a tibble keyed on ticker with: ter, isin, ytd, return_1y,
#' return_3y, return_5y, return_10y, return_3m, return_1m, return_1w
#'
#' The page renders ETF cards as repeating Markdown blocks; we extract them
#' by finding the logo URL (which always contains the ticker like
#' EQU.ZA.STX40.png) and the percentage values that follow.
scrape_easycompare_finder <- local({
  cache <- NULL
  function() {
    if (!is.null(cache)) return(cache)
    
    url <- "https://compare.easyequities.co.za/finder"
    resp <- safe_get(url, timeout = 40)
    if (inherits(resp, "fetch_fail") || status_code(resp) != 200) {
      message("    ⚠ EasyCompare finder fetch failed — falling back")
      cache <<- tibble()
      return(tibble())
    }
    
    raw <- content(resp, "text", encoding = "UTF-8")
    
    # The page is built from HubSpot; ETF cards repeat with a logo image
    # whose path encodes the ticker. Extract those first as anchors.
    # Pattern: /logos/EQU.ZA.{TICKER}.png   OR   /logos/TFSA.{TICKER}.png
    logo_pattern <- "logos/(?:EQU\\.ZA|TFSA)\\.([A-Z0-9]+)\\.png"
    matches <- str_match_all(raw, logo_pattern)[[1]]
    if (nrow(matches) == 0) {
      message("    ⚠ EasyCompare finder: no ETF cards parsed")
      cache <<- tibble()
      return(tibble())
    }
    
    tickers_in_order <- matches[, 2]
    
    # For each ticker, slice the chunk of HTML between this card and the next,
    # then pull out: TER, the named performance numbers, and the ISIN.
    positions <- str_locate_all(raw, logo_pattern)[[1]][, "start"]
    chunks <- map(seq_along(positions), function(i) {
      end <- if (i < length(positions)) positions[i+1] - 1 else nchar(raw)
      substr(raw, positions[i], end)
    })
    
    extract_pct <- function(chunk, label) {
      # Look for "**LABEL Performance:** \nNUM%" with arbitrary whitespace
      pat <- glue("\\*\\*{label} Performance:\\*\\*\\s*\\n?\\s*([\\-0-9.]+)%")
      m <- str_match(chunk, pat)
      if (!is.na(m[1, 2])) return(as.numeric(m[1, 2]) / 100)
      NA_real_
    }
    extract_ter <- function(chunk) {
      m <- str_match(chunk, "\\*\\*TER:\\*\\*\\s*([0-9.]+)")
      if (!is.na(m[1, 2])) return(as.numeric(m[1, 2]) / 100)
      NA_real_
    }
    extract_isin <- function(chunk) {
      m <- str_match(chunk, "isin=([A-Z0-9]+)")
      if (!is.na(m[1, 2])) m[1, 2] else NA_character_
    }
    extract_name <- function(chunk) {
      # The card name follows the logo image in a markdown line of plain text
      m <- str_match(chunk, "(?s)logos/[^)]+\\)\\s*\\n+\\s*([^\\n]+?)\\s*\\n")
      if (!is.na(m[1, 2])) str_trim(m[1, 2]) else NA_character_
    }
    
    out <- tibble(
      ticker      = tickers_in_order,
      ec_name     = map_chr(chunks, extract_name),
      ter         = map_dbl(chunks, extract_ter),
      isin        = map_chr(chunks, extract_isin),
      ytd_return  = map_dbl(chunks, ~extract_pct(.x, "YTD")),
      return_10y  = map_dbl(chunks, ~extract_pct(.x, "10Y")),
      return_5y   = map_dbl(chunks, ~extract_pct(.x, "5Y")),
      return_3y   = map_dbl(chunks, ~extract_pct(.x, "3Y")),
      return_1y   = map_dbl(chunks, ~extract_pct(.x, "1Y")),
      return_3m   = map_dbl(chunks, ~extract_pct(.x, "3M")),
      return_1m   = map_dbl(chunks, ~extract_pct(.x, "1M")),
      return_1w   = map_dbl(chunks, ~extract_pct(.x, "1W"))
    ) |> distinct(ticker, .keep_all = TRUE)
    
    message(glue("    ✓ EasyCompare finder: parsed {nrow(out)} ETFs"))
    cache <<- out
    out
  }
})

#' Scrape the EasyETFs instrument page for AMETFs published by
#' EasyAssetManagement (Cloud Atlas). Slug is e.g. 'easyai', 'cartbl', 'easybf'.
#' Returns full holdings table + benchmark + classification + manager fee.
scrape_easyetf_page <- function(slug) {
  url <- glue("https://etfs.easyequities.co.za/easyetf-instrument-page/{slug}")
  resp <- safe_get(url, timeout = 25)
  if (inherits(resp, "fetch_fail")) {
    return(list(success = FALSE, error = glue("network: {resp$error}")))
  }
  if (status_code(resp) != 200) {
    return(list(success = FALSE, error = glue("HTTP {status_code(resp)}")))
  }
  
  raw <- content(resp, "text", encoding = "UTF-8")
  
  pluck <- function(label) {
    # Field labels appear as headings, with the value on the line below
    pat <- glue("(?s){label}\\s*\\n+\\s*([^\\n]+?)\\s*\\n")
    m <- str_match(raw, pat)
    if (!is.na(m[1, 2])) str_trim(m[1, 2]) else NA_character_
  }
  
  benchmark      <- pluck("Benchmark")
  classification <- pluck("Classification")
  distribution   <- pluck("Distribution Dates")
  asset_manager  <- pluck("Asset Manager")
  risk_profile   <- pluck("Risk Profile")
  mgmt_fee_str   <- pluck("Management Fee \\(including VAT\\)")
  
  # NAV / price snapshot
  price_str      <- pluck("Price / NAV \\(ZAC\\)")
  fund_size_str  <- pluck("Fund Size \\(ZAR\\)")
  
  # Full holdings table — rendered as a markdown table with columns
  # Instrument | Currency | Weight
  holdings <- list()
  table_match <- str_match(raw, "(?s)\\| Instrument.*?Weight \\|\\s*\\n.*?\\n((?:\\|.*?\\n)+)")
  if (!is.na(table_match[1, 2])) {
    table_body <- table_match[1, 2]
    rows <- str_split(table_body, "\\n")[[1]] |> discard(~!str_detect(.x, "\\|"))
    for (row in rows) {
      cells <- str_split(row, "\\|")[[1]] |> str_trim()
      cells <- cells[cells != ""]
      if (length(cells) >= 3) {
        weight_str <- cells[length(cells)]
        weight <- as.numeric(str_remove_all(weight_str, "[^0-9.]"))
        if (!is.na(weight) && weight > 0) {
          holdings[[length(holdings) + 1]] <- list(
            name = cells[1],
            currency = cells[2],
            weight = weight
          )
        }
      }
    }
  }
  
  parse_pct <- function(s) {
    if (is.na(s)) return(NA_real_)
    n <- as.numeric(str_remove_all(s, "[^0-9.]"))
    if (!is.na(n)) n / 100 else NA_real_
  }
  parse_num <- function(s) {
    if (is.na(s)) return(NA_real_)
    as.numeric(str_remove_all(s, "[^0-9.\\-]"))
  }
  
  list(
    success        = TRUE,
    benchmark      = benchmark,
    classification = classification,
    distribution   = distribution,
    asset_manager  = asset_manager,
    risk_profile   = risk_profile,
    mgmt_fee       = parse_pct(mgmt_fee_str),
    price          = parse_num(price_str),
    fund_size      = parse_num(fund_size_str),
    holdings       = holdings
  )
}


#' Pages live at https://justonelap.com/{lowercase_ticker}/ and contain a
#' standard 2-column table with: Type, JSE code, Benchmark, Classification,
#' Tax-free investing, Market cap, TIC/TER, Distribution, Top holdings,
#' MDD updated, Description.
scrape_jol <- function(ticker, slug = NULL) {
  url_slug <- if (!is.null(slug) && nzchar(slug)) slug else tolower(ticker)
  url <- glue("https://justonelap.com/{url_slug}/")
  resp <- safe_get(url)
  if (inherits(resp, "fetch_fail")) {
    return(list(success = FALSE, error = glue("network: {resp$error}")))
  }
  if (status_code(resp) != 200) {
    return(list(success = FALSE, error = glue("HTTP {status_code(resp)}")))
  }
  
  page <- tryCatch(read_html(content(resp, "text", encoding = "UTF-8")),
                   error = function(e) NULL)
  if (is.null(page)) return(list(success = FALSE, error = "html parse failed"))
  
  # Find all tables and pick the one with "JSE code" in its content
  tbls <- tryCatch(html_elements(page, "table"), error = function(e) list())
  if (length(tbls) == 0) return(list(success = FALSE, error = "no tables"))
  
  jol_tbl <- NULL
  for (tbl in tbls) {
    txt <- html_text(tbl)
    if (str_detect(txt, "JSE code") && str_detect(txt, regex("Benchmark", ignore_case = TRUE))) {
      jol_tbl <- tbl
      break
    }
  }
  if (is.null(jol_tbl)) return(list(success = FALSE, error = "no JOL data table"))
  
  # The JOL table has rows where each cell contains a "Field: Value" string.
  # Parse all bolded labels and the text after them.
  cells <- html_elements(jol_tbl, "td")
  cell_texts <- map_chr(cells, ~html_text(.x, trim = TRUE))
  
  fields <- list()
  for (txt in cell_texts) {
    # Multiple "Label: value" pairs may be in one cell separated by line breaks
    pairs <- str_split(txt, "(?<=\\.)\\s*(?=[A-Z][a-z])|\\n")[[1]] |> str_trim() |> discard(~.x == "")
    for (p in pairs) {
      m <- str_match(p, "^([^:]+?):\\s*(.+)$")
      if (!is.na(m[1,2])) {
        label <- str_trim(m[1,2])
        value <- str_trim(m[1,3])
        # Normalise the label
        key <- label |> tolower() |> str_replace_all("[^a-z0-9]+", "_") |> str_remove_all("_+$")
        if (!is.null(fields[[key]])) next  # keep first occurrence
        fields[[key]] <- value
      }
    }
  }
  
  if (length(fields) == 0) return(list(success = FALSE, error = "no fields parsed"))
  
  list(
    success         = TRUE,
    benchmark       = fields$benchmark %||% NA_character_,
    classification  = fields$classification %||% NA_character_,
    type            = fields$type %||% NA_character_,
    tax_free        = fields$tax_free_investing %||% NA_character_,
    market_cap      = fields$market_cap %||% NA_character_,
    ter             = fields$tic_ter_where_tic_not_indicated %||%
      fields$tic %||% fields$ter %||% NA_character_,
    distribution    = fields$distribution %||% NA_character_,
    top_holdings    = fields$top_holdings %||% NA_character_,
    mdd_updated     = fields$mdd_updated %||% NA_character_,
    description     = fields$description %||% NA_character_
  )
}

#' Convert a raw "top holdings" comma-separated string into a list
#' of {name, weight} entries. Weights are NA when JOL doesn't publish them
#' (most cases) — the front-end handles missing weights gracefully.
parse_holdings_str <- function(s) {
  if (is.null(s) || is.na(s) || nchar(s) < 2) return(list())
  # Split on common separators
  items <- str_split(s, ",|·|·|/")[[1]] |> str_squish() |> discard(~.x == "")
  if (length(items) == 0) return(list())
  
  # Detect "Name 5.4%" pattern within each item
  parsed <- map(items, function(it) {
    m <- str_match(it, "^(.+?)\\s+([0-9]+\\.?[0-9]*)\\s*%?$")
    if (!is.na(m[1,2])) {
      list(name = str_trim(m[1,2]), weight = as.numeric(m[1,3]))
    } else {
      list(name = it, weight = NA_real_)
    }
  })
  
  parsed |> keep(~nchar(.x$name) > 1)
}

#' Convert TER strings like "0.10%" / "0.25 %" / "10 bps" → numeric (decimal).
parse_ter <- function(s) {
  if (is.null(s) || is.na(s)) return(NA_real_)
  s <- str_trim(s)
  if (str_detect(s, "bps")) {
    x <- as.numeric(str_remove_all(s, "[^0-9.]"))
    return(x / 10000)
  }
  if (str_detect(s, "%")) {
    x <- as.numeric(str_remove_all(s, "[^0-9.]"))
    return(x / 100)
  }
  x <- as.numeric(str_remove_all(s, "[^0-9.]"))
  if (!is.na(x) && x > 1) x <- x / 100
  x
}

# =============================================================================
# PRICE FETCHER — Yahoo primary, Stooq fallback
# =============================================================================

fetch_prices <- function(yf_ticker) {
  out <- tryCatch({
    suppressWarnings(p <- tq_get(yf_ticker, from = START_DATE, to = END_DATE))
    if (is.null(p) || !is.data.frame(p) || nrow(p) < MIN_ROWS) NULL else
      list(data = p, source = "yahoo")
  }, error = function(e) NULL)
  if (!is.null(out)) return(out)
  
  out <- tryCatch({
    suppressWarnings(suppressMessages(
      x <- getSymbols(yf_ticker, src = "stooq", from = START_DATE, to = END_DATE,
                      auto.assign = FALSE)
    ))
    if (is.null(x) || nrow(x) < MIN_ROWS) NULL else
      list(
        data = tibble(
          symbol = yf_ticker, date = as.Date(zoo::index(x)),
          open = as.numeric(x[,1]), high = as.numeric(x[,2]),
          low = as.numeric(x[,3]),  close = as.numeric(x[,4]),
          volume = as.numeric(x[,5]),
          adjusted = as.numeric(x[,4])
        ),
        source = "stooq"
      )
  }, error = function(e) NULL)
  out
}

# =============================================================================
# ANALYTICS HELPERS
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

message(glue("\n{'='<70}"))
message(glue(" Easy Equities TFSA ETF Pipeline  |  {format(Sys.time(),'%Y-%m-%d %H:%M')}"))
message(glue(" ETFs: {nrow(etf_csv)}  |  Range: {START_DATE} → {END_DATE}"))
message(glue(" Output: {DATA_DIR}"))
message(glue("{'='<70}\n"))

summary_rows <- list()
processed    <- character(0)
no_prices    <- character(0)
scrape_fails <- character(0)
ec_used      <- character(0)
easyetf_used <- character(0)

# Pre-fetch the EasyCompare Finder once (covers ALL ETFs in a single call)
message("Fetching EasyCompare Finder (one-time bulk fetch)...")
ec_data <- scrape_easycompare_finder()
ec_lookup <- if (nrow(ec_data) > 0) {
  setNames(split(ec_data, seq_len(nrow(ec_data))), ec_data$ticker)
} else list()
message("")

for (i in seq_len(nrow(etf_csv))) {
  row <- etf_csv[i, ]
  ticker <- row$ticker
  
  message(glue("[{i}/{nrow(etf_csv)}] {ticker} — {row$name}"))
  
  # ────────────────────────────────────────────────────────────────────
  # Source 1: Just One Lap (best for Satrix/Sygnia/1nvest/10X passive)
  # ────────────────────────────────────────────────────────────────────
  jol <- tryCatch(scrape_jol(ticker, slug = row$jol_slug),
                  error = function(e) list(success=FALSE, error=e$message))
  if (jol$success) {
    message(glue("    ✓ JOL"))
  } else {
    message(glue("    ⚠ JOL failed: {jol$error}"))
    scrape_fails <- c(scrape_fails, ticker)
  }
  
  # ────────────────────────────────────────────────────────────────────
  # Source 2: EasyETFs instrument page (for Cloud Atlas / EasyAM AMETFs)
  # ────────────────────────────────────────────────────────────────────
  ee_slug <- EASYETF_PAGE_SLUGS[ticker] %||% NA
  easyetf <- if (!is.na(ee_slug) && nzchar(ee_slug)) {
    res <- tryCatch(scrape_easyetf_page(ee_slug),
                    error = function(e) list(success=FALSE, error=e$message))
    if (res$success) {
      message(glue("    ✓ EasyETFs page ({length(res$holdings)} holdings)"))
      easyetf_used <- c(easyetf_used, ticker)
    } else {
      message(glue("    ⚠ EasyETFs page failed: {res$error}"))
    }
    res
  } else NULL
  
  # ────────────────────────────────────────────────────────────────────
  # Source 3: EasyCompare row (already fetched in bulk)
  # ────────────────────────────────────────────────────────────────────
  ec_row <- ec_lookup[[ticker]]
  if (!is.null(ec_row)) {
    ec_used <- c(ec_used, ticker)
    message(glue("    ✓ EasyCompare data"))
  }
  
  # ────────────────────────────────────────────────────────────────────
  # Merge metadata — preference order: EasyETFs page → JOL → EasyCompare
  # ────────────────────────────────────────────────────────────────────
  benchmark <- coalesce(
    if (!is.null(easyetf) && easyetf$success) easyetf$benchmark else NA_character_,
    if (jol$success) jol$benchmark else NA_character_
  )
  classification <- coalesce(
    if (!is.null(easyetf) && easyetf$success) easyetf$classification else NA_character_,
    if (jol$success) jol$classification else NA_character_
  )
  description <- if (jol$success) jol$description else NA_character_
  distribution <- coalesce(
    if (!is.null(easyetf) && easyetf$success) easyetf$distribution else NA_character_,
    if (jol$success) jol$distribution else NA_character_
  )
  fund_type <- if (jol$success) jol$type else NA_character_
  mdd_updated <- if (jol$success) jol$mdd_updated else NA_character_
  
  # TER: EasyCompare is most authoritative (it's the EE platform's own data)
  ter_dec <- coalesce(
    if (!is.null(ec_row)) ec_row$ter else NA_real_,
    if (jol$success) parse_ter(jol$ter) else NA_real_
  )
  
  # Holdings: EasyETFs full table beats JOL's truncated list
  holdings <- if (!is.null(easyetf) && easyetf$success && length(easyetf$holdings) > 0) {
    easyetf$holdings
  } else if (jol$success) {
    parse_holdings_str(jol$top_holdings)
  } else list()
  
  # ────────────────────────────────────────────────────────────────────
  # Source 4: Yahoo / Stooq prices
  # ────────────────────────────────────────────────────────────────────
  px <- if (is.na(row$yf)) {
    NULL
  } else {
    fetch_prices(row$yf)
  }
  has_prices <- !is.null(px)
  
  if (has_prices) {
    message(glue("    ✓ prices: {px$source} ({nrow(px$data)} rows)"))
    metrics  <- compute_metrics(px$data)
    monthly  <- compute_monthly_returns(px$data)
    rolling  <- compute_rolling_vol(px$data)
    drawdown <- compute_drawdown_series(px$data)
    price_ts <- build_price_series(px$data)
  } else {
    if (!is.na(row$yf)) message(glue("    ✗ no price series ({row$yf})"))
    no_prices <- c(no_prices, ticker)
    metrics <- monthly <- rolling <- drawdown <- price_ts <- NULL
  }
  
  # ────────────────────────────────────────────────────────────────────
  # Build the per-ETF JSON payload
  # ────────────────────────────────────────────────────────────────────
  payload <- list(
    ticker = ticker, name = row$name, provider = row$provider,
    ee_aliases = row$ee_aliases[[1]],
    category = classification %||% "Unknown",
    ter = ter_dec, dividend = distribution,
    index = benchmark, index_rules = description,
    rebalancing = NA_character_, fund_type = fund_type,
    mdd_updated = mdd_updated,
    top_holdings = holdings,
    has_price_data = has_prices,
    last_updated = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
  if (has_prices) {
    payload$metrics <- metrics
    payload$prices <- price_ts
    payload$monthly_returns <- monthly
    payload$rolling_vol <- rolling
    payload$drawdown <- drawdown
  }
  if (!is.null(ec_row)) {
    payload$ee_isin <- ec_row$isin
  }
  write_json(payload, file.path(DATA_DIR, glue("{ticker}.json")),
             auto_unbox = TRUE, digits = 6, pretty = FALSE, na = "null")
  
  # ────────────────────────────────────────────────────────────────────
  # Summary row — analytics from prices when available, else from
  # EasyCompare's pre-computed returns. The leaderboard cards and table
  # then have valid numbers for ALL ETFs in the universe.
  # ────────────────────────────────────────────────────────────────────
  summary_rows[[ticker]] <- tibble(
    ticker     = ticker,
    name       = row$name,
    provider   = row$provider,
    category   = classification %||% NA_character_,
    ter        = ter_dec,
    dividend   = distribution,
    
    # YTD / 1Y / 3Y returns — prefer EasyCompare (the official EE figures)
    ytd_return = coalesce(
      if (!is.null(ec_row)) ec_row$ytd_return else NA_real_,
      if (has_prices) metrics$ytd_return else NA_real_
    ),
    return_1y  = coalesce(
      if (!is.null(ec_row)) ec_row$return_1y else NA_real_,
      if (has_prices) metrics$return_1y else NA_real_
    ),
    return_3y  = coalesce(
      if (!is.null(ec_row)) ec_row$return_3y else NA_real_,
      if (has_prices) metrics$return_3y else NA_real_
    ),
    
    # CAGR / vol / risk-adjusted require a price series — only available
    # for ETFs with Yahoo or Stooq coverage.
    cagr           = if (has_prices) metrics$cagr else NA_real_,
    ann_vol        = if (has_prices) metrics$ann_vol else NA_real_,
    sharpe         = if (has_prices) metrics$sharpe else NA_real_,
    sortino        = if (has_prices) metrics$sortino else NA_real_,
    calmar         = if (has_prices) metrics$calmar else NA_real_,
    max_drawdown   = if (has_prices) metrics$max_drawdown else NA_real_,
    var_95         = if (has_prices) metrics$var_95 else NA_real_,
    n_trading_days = if (has_prices) metrics$n_trading_days else 0L
  )
  
  processed <- c(processed, ticker)
  
  Sys.sleep(0.5)  # be polite to all upstream servers
}

# Master files
summary_df <- bind_rows(summary_rows)
write_json(summary_df, file.path(DATA_DIR,"summary.json"),
           auto_unbox=TRUE, digits=6, na="null")

# etf_list with aliases (for client-side fuzzy search)
etf_list_out <- etf_csv |>
  filter(ticker %in% processed) |>
  mutate(ee_aliases = map(ee_aliases, ~if (length(.x) == 0) list() else .x))

write_json(etf_list_out, file.path(DATA_DIR,"etf_list.json"),
           auto_unbox=TRUE, pretty=TRUE, na="null")

# ── Final report ──────────────────────────────────────────────────────────────
message(glue("\n{'='<70}"))
message(glue(" Pipeline complete  |  {format(Sys.time(),'%Y-%m-%d %H:%M:%S')}"))
message(glue("   Total ETFs        : {nrow(etf_csv)}"))
message(glue("   Processed         : {length(processed)}"))
message(glue("   With Yahoo prices : {length(processed) - length(no_prices)}"))
message(glue("   EasyCompare data  : {length(ec_used)} (powers leaderboard for non-Yahoo funds)"))
message(glue("   EasyETFs holdings : {length(easyetf_used)} (full constituent tables)"))
message(glue("   JOL data          : {nrow(etf_csv) - length(scrape_fails)} of {nrow(etf_csv)}"))
if (length(no_prices) > 0) {
  message(glue("\n   No Yahoo/Stooq prices ({length(no_prices)}):"))
  message(glue("     {paste(no_prices, collapse=', ')}"))
  message("   ↳ These still appear in the comparison table with EasyCompare returns.")
}
if (length(scrape_fails) > 0) {
  message(glue("\n   JOL not found ({length(scrape_fails)}):"))
  message(glue("     {paste(scrape_fails, collapse=', ')}"))
}
message(glue("\n{'='<70}\n"))

# =============================================================================
# AUTO GIT COMMIT  (uncomment to enable)
# =============================================================================
# system(glue('cd "{here()}" && git add posts/ee-etf-tracker/data/ && ',
#             'git commit -m "data: weekly ETF refresh {Sys.Date()}" && ',
#             'git push'))