-- filtro per anno e ordino per ticker e data
DROP table filterOnYearAndOrder;
CREATE TABLE filterOnYearAndOrder AS
SELECT ticker, ymd, close, volume, year
FROM hist_prices
WHERE year >= 2004 AND
year <= 2018
ORDER BY ticker, ymd;

-- prendo primo e ultimo giorno per ogni ticker, anno
DROP TABLE getFirstAndLastDay;
CREATE TABLE getFirstAndLastDay AS
SELECT ticker, min(ymd) as min_date, max(ymd) as max_date
FROM filterOnYearAndOrder
GROUP BY ticker, year;

-- prendo il prezzo iniziale per ogni ticker
DROP TABLE startPrice;
CREATE TABLE startPrice AS
SELECT f.ticker, year, f.close as start_price
FROM filterOnYearAndOrder f, getFirstAndLastDay g
WHERE f.ticker = g.ticker AND f.ymd = g.min_date;

-- prendo il prezzo finale per ogni ticker
DROP TABLE endPrice;
CREATE TABLE endPrice AS
SELECT f.ticker, year, f.close as end_price
FROM filterOnYearAndOrder f, getFirstAndLastDay g
WHERE f.ticker = g.ticker AND f.ymd = g.max_date;


DROP TABLE aggregateResult;
CREATE TABLE aggregateResult AS
SELECT ticker, year, sum(close) as sum_close, sum(volume) as sum_volume
FROM filterOnYearAndOrder
GROUP by ticker, year;

DROP TABLE variazioneResult;
CREATE TABLE variazioneResult AS
SELECT ar.ticker, ar.year, (((ep.end_price - sp.start_price)/sp.start_price)*100) as incremento_percentuale, ar.sum_close, ar.sum_volume
FROM aggregateResult ar, startPrice sp, endPrice ep
WHERE ar.ticker = sp.ticker AND ar.ticker = ep.ticker AND ar.year = sp.year AND ar.year = ep.year;

DROP TABLE finalResult;
CREATE TABLE finalResult ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' AS
SELECT hs.sector, vr.year, sum(vr.sum_volume), sum(vr.sum_close), sum(vr.incremento_percentuale)
FROM variazioneResult vr, hist_stocks hs
WHERE vr.ticker = hs.ticker 
GROUP BY hs.sector, vr.year
ORDER BY hs.sector, vr.year;
