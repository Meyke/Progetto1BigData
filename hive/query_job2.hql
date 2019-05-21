-- Creo la tabella
-- Con skip.header.line.count salto l'header del csv

DROP TABLE hist_prices;
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

LOAD DATA LOCAL INPATH '/home/daniele/Documenti/data/historical_stock_prices.csv'
OVERWRITE INTO TABLE hist_prices;

CREATE TABLE IF NOT EXISTS hist_stocks (
ticker STRING,
exchange_1 STRING,
name STRING,
sector STRING,
industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"");

LOAD DATA LOCAL INPATH '/home/daniele/Documenti/data/historical_stocks.csv' 
OVERWRITE INTO TABLE hist_stocks;

-- filtro per anno e ordino per ticker e data
DROP table filterOnYearAndOrder;
CREATE TABLE filterOnYearAndOrder AS
SELECT ticker, ymd, close, volume
FROM hist_prices
WHERE YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(ymd, 'yyyy-MM-dd')))) >= 2004 AND
YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(ymd, 'yyyy-MM-dd')))) <= 2018
ORDER BY ticker, ymd;

-- prendo primo e ultimo giorno per ogni ticker, anno
DROP TABLE getFirstAndLastDay;
CREATE TABLE getFirstAndLastDay AS
SELECT ticker, min(ymd) as min_date, max(ymd) as max_date
FROM filterOnYearAndOrder
GROUP BY ticker, YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(ymd, 'yyyy-MM-dd'))));

-- prendo il prezzo iniziale per ogni ticker
DROP TABLE startPrice;
CREATE TABLE startPrice AS
SELECT f.ticker, YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(ymd, 'yyyy-MM-dd')))) as year, f.close as start_price
FROM filterOnYearAndOrder f, getFirstAndLastDay g
WHERE f.ticker = g.ticker AND f.ymd = g.min_date;

-- prendo il prezzo finale per ogni ticker
DROP TABLE endPrice;
CREATE TABLE endPrice AS
SELECT f.ticker, YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(ymd, 'yyyy-MM-dd')))) as year, f.close as end_price
FROM filterOnYearAndOrder f, getFirstAndLastDay g
WHERE f.ticker = g.ticker AND f.ymd = g.max_date;


DROP TABLE aggregateResult;
CREATE TABLE aggregateResult AS
SELECT ticker, YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(ymd, 'yyyy-MM-dd')))) as year, sum(close) as sum_close, sum(volume) as sum_volume
FROM filterOnYearAndOrder
GROUP by ticker, YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(ymd, 'yyyy-MM-dd'))));

DROP TABLE variazioneResult;
CREATE TABLE variazioneResult AS
SELECT ar.ticker, ar.year, (((ep.end_price - sp.start_price)/sp.start_price)*100) as incremento_percentuale, ar.sum_close, ar.sum_volume
FROM aggregateResult ar, startPrice sp, endPrice ep
WHERE ar.ticker = sp.ticker AND ar.ticker = ep.ticker AND ar.year = sp.year AND ar.year = ep.year;

DROP TABLE finalResult;
CREATE TABLE finalResult ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' AS
SELECT hs.sector, vr.year, sum(vr.sum_volume), sum(vr.sum_close), sum(vr.incremento_percentuale)
FROM variazioneResult vr, hist_stocks hs
WHERE vr.ticker = hs.ticker 
GROUP BY hs.sector, vr.year
ORDER BY hs.sector, vr.year;
