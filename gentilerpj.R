
rm(list=ls(all=T))
#----------------------------------------------------------
t1 <- Sys.time(); print(t1) ###############################
#----------------------------------------------------------

options(java.parameters="-Xmx5g")
source("src/dateFunctions.R")
source("src/VJP_Utils.r")

library(stringr)
library(RODBC)
library(dplyr)
library(magrittr)

odbcCloseAll()

# Inputs ------------------------------------------------------------------

startDate   <- "2017-01-01" # First date of the date range to process in yyyy-mm-dd format
endDate     <- "2017-12-31" # Last date of the date range to process in yyyy-mm-dd format
wtStartDate <- "2017-10-01" # First date of worst trades date range in yyyy-mm-dd format
wtEndDate   <- endDate # Last date of worst trades date range in yyyy-mm-dd format
minqty      <- 500000 # Smallest allowable size for a trade
minCnt      <- 2 # Minimum number of existing Trace trades for a isin to be included
sprdFloor   <- 40 # Ignore trades whose comparable trades have an average spread of less than sprdFloor
sprdCeil    <- 2000 # Ignore trades whose comparable trades have an average spread of sprdCeil or more

tracePath <- "X:/FSGNT/TraceData/"
prefix    <- "mabondticker-"
fileExt   <- ".csv"

yrTraceDF <- NULL


dates <- as.character(seq(as.Date(startDate), as.Date(endDate), by = 1), format = '%Y%m%d')
#drv <- JDBC("com.sybase.jdbc4.jdbc.SybDriver", "C:/Sybase/jConnect-7_0/classes/jconn4.jar")
#conn <- dbConnect(drv, "jdbc:sybase:Tds:frmlxdb1p1:6100/db_firmprod", "x141862", "x141862")
conn=odbcConnect(dsn="TMON_FIRM_P1", uid = "x209816", pwd = "exk1rbut", believeNRows=FALSE) 

mktSeg = "Emerging Markets"


for (trdDt in dates) {
  
  # Trace Data Manipulation --------------------------------------------------------------------
  
  # Determine current trade date
  file1 = paste0(tracePath, prefix, trdDt, fileExt)
  if (!file.exists(file1)) { next }
  
  # Read in and manipulate Trace data for current trade date
  df1 = read.csv(file1, as.is = TRUE)
  df1 = df1 %>%
    select(isin = ISIN, price = PRICE, qty = QUANTITY, estqty = ESTIMATEDQUANTITY, type = TRADE_TYPE,
           spread = OASSPREAD, seg = MARKETSEGMENT, cflag = CANCELFLAG, effdate = EFFECTIVEDATE,
           efftime = EFFECTIVETIME) %>%
    mutate(datetime = as.POSIXct(strptime(paste(effdate, efftime), format = "%m/%d/%Y %H:%M:%S", tz = "EST"))) %>%
    mutate(spread = as.numeric(spread)) %>%
    filter(estqty >= minqty) %>%
    filter(seg == mktSeg) %>%
    filter(type == "B" | type == "S") %>%
    mutate(type = ifelse(type == "S", "BUY", "SELL")) %>%
    filter(cflag == "N") %>%
    mutate(est = (qty != estqty), date = as.Date(trdDt, format = "%Y%m%d")) %>%
    select(date, isin, price, estqty, type, spread, datetime, est, effdate, seg)
  
  df1 <- df1 %>% 
    group_by(isin, price, estqty, effdate) %>%
    arrange(datetime) %>%
    mutate(tradeNum = cumsum(price/price)) 
  
  dfPreviousTrade <- df1 %>%
    select(isin, price, estqty, type, datetime, spread, tradeNum, effdate) %>% 
    mutate(tradeNum = tradeNum + 1) %>%
    mutate(PrevTime = datetime, PrevType = type, PrevSpread = spread) %>%
    ungroup() %>%
    select(-datetime, -type, -spread)
  
  df1 %<>% left_join(dfPreviousTrade) %>%
    mutate(timeDiff = ifelse(is.na(PrevTime), 100, difftime(datetime, PrevTime, units = "mins")))
  
  dfPreviousTrade <- df1 %>%
    filter(!is.na(timeDiff)) %>%
    select(isin, price, estqty, type, datetime, spread, tradeNum, effdate) %>%
    mutate(NextTrade = tradeNum, tradeNum = tradeNum - 1, NextTradeTime = datetime) %>%
    ungroup() %>%
    select(-datetime, -type, -spread)
  
  df1 <- df1 %>% 
    left_join(dfPreviousTrade) %>%
    mutate(double = ifelse(tradeNum > 1 | !is.na(NextTrade), "Y", "N")) %>%
    mutate(timeDiff2 = ifelse(is.na(NextTradeTime), 100, difftime(NextTradeTime, datetime, units = "mins"))) %>%
    ungroup() 
  
  # Append today's Trace data frame to final Trace data frame covering the whole year
  if (is.null(yrTraceDF)) { yrTraceDF <- df1 } else { yrTraceDF <- rbind(yrTraceDF, df1) }
  trdDt}

traxfinalbuydf <- yrTraceDF[1:5] %>% filter(type == "BUY") %>% group_by(isin, date) %>% arrange(date, isin) %>%mutate(n=n()) %>% filter(n>= minCnt) %>% summarise(avgprice = mean(price), n=n()) %>% arrange(date)

traxfinalselldf <- yrTraceDF[1:5] %>% filter(type == "SELL") %>% group_by(isin, date) %>% arrange(date, isin) %>%mutate(n=n()) %>% filter(n>= minCnt) %>% summarise(avgprice = mean(price), n=n()) %>% arrange(date)

#----------------------------------------------------------------------------------------------------------------














dir = "X:/FSGNT/PAG QRRM/Execution/TRAX/Data/"
months = c("Jan17","Feb17","Mar17", "Apr17", "May17", "Jun17", "Jul17","Aug17","Sep17","Oct16","Nov16","Dec16")

traxDF = NULL
wtTraxDF <- NULL
### This loop constructs a data frame of comparable trades over the entire year
for (month in months) {
  # Read in this month's data
  dataPath = paste0(dir, month, fileExt)
  data <- read.csv(dataPath, as.is = TRUE)
  
  ## Data manipulation
  data <- data %>%
    mutate(TRADETIME = chron(times = TRADETIME)) %>%
    mutate(TRADETIME = replace(TRADETIME, which(is.na(TRADETIME)), chron(times = "17:00:00"))) %>%
    mutate(TRADEDATE = as.Date(TRADEDATE, format = "%d/%m/%Y") - 1 * (TRADETIME < chron(times = "5:00:00"))) %>%
    mutate(PRICE = as.numeric(PRICE), VOLUMEUSD = as.numeric(VOLUMEUSD)) %>%
    filter(INSTRUMENTCATEGORY == "Emerging") %>%
    filter(VOLUMEUSD >= minqty | is.na(VOLUMEUSD)) %>% # Remove trades under the quantity threshold
    filter(SECURITYTYPE == "S") %>% # Do not include FRNs, convertibles, or bills
    filter(!is.na(PRICE)) %>%
    filter(PRINCIPALROLE == "DEALER") %>%
    filter(COUNTERPARTYROLE != "DEALER")
  
  #if (ccyType == "Hard") {
   # data <- data %>%
    #  filter(TRADEDCCY %in% c("EUR","GBP","JPY","USD"))
  #} else if (ccyType == "Local") {
   # data <- data %>%
    #  filter(!(TRADEDCCY %in% c("AUD","CAD","CHF","DKK","EUR","GBP","ITL","JPY",
     #                           "NOK","NZD","SEK","USD")))
  #} else stop("Invalid Currency Type")
  
  # Calculate avg price and number of trades for every date, isin, and buy/sell combination
  wtData <- data %>% 
    mutate(type=BUYSELL, trd_trade_dt = TRADEDATE, isin = ISIN) %>% 
    select(TRADEID,TRADEACTION,SECURITYID,ISIN,TRADEDATE,TRADETIME,PRICE,PRICEQUALITY,SPREADBMK,BMKISIN,TRADEDCCY,VOLUMECCY,VOLUMEEUR,VOLUMEUSD,BUYSELL,PRINCIPALROLE,COUNTERPARTYROLE,SECURITYTYPE,MATURITYDATE,ISSUEDATE,ISSUECCY,COUPON,COUPONFREQ,ISSUEDCCY,ISSUEDEUR,ISSUEDUSD,OUTSTANDCCY,OUTSTANDEUR,OUTSTANDUSD,INSTRUMENTCATEGORY,ISSUER,ISSUERNLTY,GUARANTORNLTY,INDUSTRYCODE,type,trd_trade_dt,isin)
  
  data <- data %>%
    group_by(trdDt = TRADEDATE, isin = ISIN, type = BUYSELL) %>%
    dplyr::summarize(n = n(), avgPrice = mean(PRICE), avgTime = mean(TRADETIME)) %>%
    filter(n >= minCnt)
  
  # Append this month's data to final data frame
  if (is.null(traxDF)) {traxDF <- data} else {traxDF <- rbind.data.frame(traxDF, data)}
  if (is.null(wtTraxDF)) {wtTraxDF <- wtData} else {wtTraxDF <- rbind.data.frame(wtTraxDF, wtData)}
  
}