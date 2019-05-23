package job3

import job1.{DateOrdering, StockPrice}
import logger.MyLogger
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD



object ThreeYearTrend {

  // Create the spark configuration and spark context
  val conf = new SparkConf()
    .setAppName("Job1")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)

  var pathToFile1 = "/Users/micheletedesco1/Desktop/job1/historical_stock_prices.csv"
  var pathToFile2 = "/Users/micheletedesco1/Desktop/job1/historical_stocks.csv"
  var outputPath = "/Users/micheletedesco1/Desktop/risultati-screenshot/output"

  val creaStockRDD = (st: Array[String]) => {
    StockPrice(st(0), st(1).toFloat, st(2).toFloat, st(3).toFloat, st(4).toFloat, st(5).toFloat, st(6).toLong, st(7))

  }

  def calcolaQuotazione(prices: Iterable[StockPrice]): Float = {
    if (prices.size == 0) //vuol dire che mancano le quotazioni per quell'anno
      return 0

    var prezzoFinale = prices.max(DateOrdering)
    var prezzoIniziale = prices.min(DateOrdering)
    val variazioneInizioFine: Float = (prezzoFinale.close - prezzoIniziale.close) / prezzoIniziale.close * 100

    (Math.round(variazioneInizioFine))
  }

  val calcolaQuotazioneTriennio = (collezionePrice: Iterable[StockPrice]) => {
    var variazione2016 = calcolaQuotazione(collezionePrice.filter((_.ymd.contains("2016"))))
    var variazione2017 = calcolaQuotazione(collezionePrice.filter((_.ymd.contains("2017"))))
    var variazione2018 = calcolaQuotazione(collezionePrice.filter((_.ymd.contains("2018"))))

    (variazione2016, variazione2017, variazione2018)
  }


  def loadData1(): RDD[(String, StockPrice)] = {

    // Load the data and parse it into a StockPrice.
    // Look at case class below
    val stocks_price = sc.textFile(pathToFile1)
      .filter(_.contains("ticker,open,close,adj_close,low,high,volume,date") == false) // filtro la prima riga
      .map(record => record.split(","))
      .map(x => creaStockRDD(x))
      .filter((x: StockPrice) => x.ymd.contains("2016") || x.ymd.contains("2017") || x.ymd.contains("2018"))
      .map(x => (x.ticker, x))
    stocks_price
  }

  def creaHistoricalStock(st: Array[String]): HistoricalStock = {
    HistoricalStock(st(0), st(1), st(2), st(3), st(4))
  }

  def loadData2(): RDD[(String, HistoricalStock)] = {

    // Load the data and parse it into a StockPrice.
    // Look at case class below
    val historical_stocks = sc.textFile(pathToFile2)
      .filter(_.contains("ticker,exchange,name,sector,industry") == false) // filtro la prima riga
      .map(record => record.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
      .map(x => creaHistoricalStock(x))
      .map(x => (x.ticker, x))
    historical_stocks
  }

  def calcolaTrendTriennioTicker(): RDD[(String, (Float, Float, Float))] = {

    val stocks_price = loadData1().partitionBy(new HashPartitioner(4))

    val quotazioneTriennio = stocks_price.groupByKey()
      .map(t => (t._1, calcolaQuotazioneTriennio(t._2)))

    quotazioneTriennio

  }


  def generatoreCoppie(stessoTrend: Iterable[(String, TrendStock)]): List[(String, String)] = {
    val elist = stessoTrend.toList
    var p1 = List[(String, String)]()
    var i = 0
    var j = 0
    for (i <- 0 to elist.size-1){
      var x = elist(i)
      for (j <- i+1 to elist.size-1){
        var y = elist(j)
        if (x._2.sector != y._2.sector){
          p1 = (x._2.name, y._2.name) :: p1
        }
      }
    }
   p1
  }

  def coppieAziendeSettoriDiversiTrend() = {
    val trendStock = calcolaTrendTriennioTicker()
    val historical_stocks = loadData2().partitionBy(new HashPartitioner(4))

    val rddJoin = trendStock.join(historical_stocks).map(x => (x._1, TrendStock(x._1, x._2._1._1, x._2._1._2, x._2._1._3, x._2._2.sector, x._2._2.name)))
    val rddCoppie = rddJoin.map(x => (x._2.trend2016 +  " " + x._2.trend2017  + " " + x._2.trend2018, x))
        .groupByKey()
        .filter(x => x._1.contains("0.0 0.0 0.0") == false)
        .mapValues(x => generatoreCoppie(x))
        .flatMap{ case (key, values) => values.map((key, _)) }
    rddCoppie

  }


  case class HistoricalStock(
                              ticker: String,
                              exchange: String,
                              name: String,
                              sector: String,
                              industry: String
                            )

  case class TrendStock(
                         ticker: String,
                         trend2016: Float,
                         trend2017: Float,
                         trend2018: Float,
                         sector: String,
                         name: String
                       )

  def main(args: Array[String]): Unit = {
    var startTime = System.currentTimeMillis()

    val log = new MyLogger(this.getClass, 3)
    log.appenderLogger()

    pathToFile1 = args(0)
    pathToFile2 = args(1)
    outputPath = args(2)

    ThreeYearTrend.coppieAziendeSettoriDiversiTrend().coalesce(1).saveAsTextFile(outputPath)

    log.timeLog((System.currentTimeMillis() - startTime) / 1000.0)
    //ThreeYearTrend.coppieAziendeSettoriDiversiTrend().take(8000).foreach(println)

  }

}
