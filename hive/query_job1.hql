-- Creo la tabella
-- Con skip.header.line.count salto l'header del csv

CREATE EXTERNAL TABLE hist_prices (
ticker STRING,
open FLOAT,
close FLOAT,
adj_close FLOAT,
low FLOAT,
high FLOAT,
volume INT,
ymd STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
tblproperties ("skip.header.line.count"="1");


-- carico il csv (la tabella punterà a quel csv)

LOAD DATA LOCAL INPATH '/Users/micheletedesco1/Desktop/job1/historical_stock_prices.csv'
OVERWRITE INTO TABLE hist_prices;


-- filtro per anno e ordino per ticker e data
DROP table filterOnYearAndOrder;
CREATE TABLE filterOnYearAndOrder AS
SELECT ticker, ymd, close, volume
FROM hist_prices
WHERE YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(ymd, 'yyyy-MM-dd')))) > 1998 AND
YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(ymd, 'yyyy-MM-dd')))) < 2018
ORDER BY ticker, ymd;

-- prendo primo e ultimo giorno per ogni ticker
DROP TABLE getFirstAndLastDay;
CREATE TABLE getFirstAndLastDay AS
SELECT ticker, min(ymd) as min_date, max(ymd) as max_date
FROM filterOnYearAndOrder
GROUP BY ticker;

-- prendo il prezzo iniziale per ogni ticker
DROP TABLE startPrice;
CREATE TABLE startPrice AS
SELECT f.ticker, f.close as start_price
FROM filterOnYearAndOrder f, getFirstAndLastDay g
WHERE f.ticker = g.ticker AND f.ymd = g.min_date;

-- prendo il prezzo finale per ogni ticker
DROP TABLE endPrice;
CREATE TABLE endPrice AS
SELECT f.ticker, f.close as end_price
FROM filterOnYearAndOrder f, getFirstAndLastDay g
WHERE f.ticker = g.ticker AND f.ymd = g.max_date;

-- aggrego per ticker
DROP TABLE aggregateResult;
CREATE TABLE aggregateResult AS
SELECT ticker, min(close) as min_low, max(close) as max_high, (sum(volume)/count(*)) as mean_volume
FROM filterOnYearAndOrder
GROUP by ticker;

-- join con prezzo iniziale e finale e calcolo incremento percentuale
DROP TABLE finalResult;
CREATE TABLE finalResult AS
SELECT ar.ticker, (((ep.end_price - sp.start_price)/sp.start_price)*100) as incremento_percentuale, ar.min_low, ar.max_high, ar.mean_volume
FROM aggregateResult ar, startPrice sp, endPrice ep
WHERE ar.ticker = sp.ticker AND ar.ticker = ep.ticker
ORDER BY incremento_percentuale desc limit 10;
