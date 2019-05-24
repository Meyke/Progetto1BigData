-- filtro per anno e ordino per ticker e data
DROP table filterOnYearAndOrder;
CREATE TABLE filterOnYearAndOrder AS
SELECT ticker, ymd, close, year
FROM hist_prices
WHERE year >= 2016 AND year <= 2018
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
SELECT f.ticker, f.year, f.close as start_price
FROM filterOnYearAndOrder f, getFirstAndLastDay g
WHERE f.ticker = g.ticker AND f.ymd = g.min_date;

-- prendo il prezzo finale per ogni ticker
DROP TABLE endPrice;
CREATE TABLE endPrice AS
SELECT f.ticker, f.year, f.close as end_price
FROM filterOnYearAndOrder f, getFirstAndLastDay g
WHERE f.ticker = g.ticker AND f.ymd = g.max_date;

DROP TABLE variazioneFor2016Result;
CREATE TABLE variazioneFor2016Result AS
SELECT sp.ticker, sp.year, (((ep.end_price - sp.start_price)/sp.start_price)*100) as variazione_percentuale
FROM startPrice sp, endPrice ep
WHERE sp.ticker = ep.ticker AND sp.year = 2016 AND ep.year = 2016;

DROP TABLE variazioneFor2017Result;
CREATE TABLE variazioneFor2017Result AS
SELECT sp.ticker, sp.year, (((ep.end_price - sp.start_price)/sp.start_price)*100) as variazione_percentuale
FROM startPrice sp, endPrice ep
WHERE sp.ticker = ep.ticker AND sp.year = 2017 AND ep.year = 2017;

DROP TABLE variazioneFor2018Result;
CREATE TABLE variazioneFor2018Result AS
SELECT sp.ticker, sp.year, (((ep.end_price - sp.start_price)/sp.start_price)*100) as variazione_percentuale
FROM startPrice sp, endPrice ep
WHERE sp.ticker = ep.ticker AND sp.year = 2018 AND ep.year = 2018;

DROP TABLE variazioneResult;
CREATE TABLE variazioneResult ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"") AS
SELECT hs.name as name, hs.sector as sector, v1.year as year1, round(v1.variazione_percentuale,0) as variazione1, v2.year as year2, round(v2.variazione_percentuale,0) as variazione2, v3.year as year3, round(v3.variazione_percentuale,0) as variazione3
FROM variazioneFor2016Result v1, variazioneFor2017Result v2, variazioneFor2018Result v3, hist_stocks hs
WHERE v1.ticker = v2.ticker AND v2.ticker = v3.ticker AND v1.ticker = hs.ticker;

DROP TABLE finalResult;
CREATE TABLE finalResult ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"") AS
SELECT vr1.name as name1, vr2.name as name2, vr1.variazione1, vr1.variazione2, vr1.variazione3
FROM variazioneResult vr1, variazioneResult vr2
WHERE vr1.variazione1 = vr2.variazione1 AND vr1.variazione2 = vr2.variazione2 AND vr1.variazione3 = vr2.variazione3 AND vr1.sector <> vr2.sector AND vr1.name <> vr2.name
ORDER BY vr1.variazione1, vr1.variazione2, vr1.variazione3;
