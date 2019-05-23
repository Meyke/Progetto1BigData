set hive.exec.dynamic.partition.mode=nonstrict

DROP TABLE hist_stocks;
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
LOAD DATA LOCAL INPATH './home/daniele/Documenti/data/historical_stocks.csv' OVERWRITE INTO TABLE hist_stocks;

DROP TABLE hist_prices;
CREATE TABLE hist_prices (
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
LOAD DATA LOCAL INPATH './home/daniele/Documenti/data/historical_stock_prices.csv' OVERWRITE INTO TABLE hist_prices;
