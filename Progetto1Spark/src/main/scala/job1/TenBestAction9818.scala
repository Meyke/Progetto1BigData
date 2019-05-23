package job1

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import java.time.LocalDate

import logger.MyLogger

// dichiararlo object per non avere errori di serializzazione.
// https://docs.azuredatabricks.net/spark/latest/rdd-streaming/developing-streaming-applications.html
object TenBestAction9818 {


  //val pathToFile = "/Users/micheletedesco1/Desktop/job1/historical_stock_prices.csv"
  var pathToFile: String = ""

  val creaStockRDD = (st: Array[String]) => {
    StockPrice(st(0), st(1).toFloat, st(2).toFloat, st(3).toFloat, st(4).toFloat, st(5).toFloat, st(6).toLong, st(7))

  }

  def filtroAnno(x: StockPrice): Boolean = {
    var data = LocalDate.parse(x.ymd)
    return (data.getYear() >= 1998 && data.getYear() <= 2018)

  }

  /**
    * Load the data from the csv and return an RDD StockPrice
    */
  def loadData(): RDD[(String, StockPrice)] = {

    // Create the spark configuration and spark context
    val conf = new SparkConf()
      .setAppName("Job1")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // Load the data and parse it into a StockPrice.
    // Look at case class below
    val stocks = sc.textFile(pathToFile)
      .filter(_.contains("ticker,open,close,adj_close,low,high,volume,date") == false) // filtro la prima riga
      .map(record => record.split(","))
      .map(x => creaStockRDD(x))
      .filter(x => filtroAnno(x))
      .map(x => (x.ticker, x))
    stocks
  }

  val calcola = (collezionePrice: Iterable[StockPrice]) => {
    var prezzoFinale = collezionePrice.max(DateOrdering)
    var prezzoIniziale = collezionePrice.min(DateOrdering)
    val variazioneInizioFine: Float = (prezzoFinale.close - prezzoIniziale.close) / prezzoIniziale.close * 100

    val massimo = collezionePrice.map(x => x.high).reduce((x, y) => Math.max(x, y))
    val minimo = collezionePrice.map(x => x.low).reduce((x, y) => Math.min(x, y))
    val volumeGiornaliero: Float = collezionePrice.map(x => x.volume).reduce((x, y) => x + y) / collezionePrice.size
    ((Math.round(variazioneInizioFine * 100.0) / 100.0).toFloat, massimo, minimo, (Math.round(volumeGiornaliero * 100.0) / 100.0).toFloat)
  }

  def incrementoPercentuale(): RDD[(String, (Float, Float, Float, Float))] = {
    val stocks = loadData.partitionBy(new HashPartitioner(4))

    stocks.groupByKey() //il groupby impiega tempo (infatti non fa una combine)
      .mapValues(calcola(_))

  }

  /*
*  Top 10 stocks
*/
  def topTenStocks(): Array[(String, (Float, Float, Float, Float))] = {
    val data = incrementoPercentuale

    data.sortBy (_._2._1, false, 1)
      .take (10)
  }

  def main(args: Array[String]): Unit = {

    var startTime = System.currentTimeMillis()

    val log = new MyLogger(this.getClass, 1)
    log.appenderLogger()

    TenBestAction9818.pathToFile = args(0)


    TenBestAction9818.topTenStocks().foreach(println)

    log.timeLog((System.currentTimeMillis() - startTime) / 1000.0)

    //while(true){

    //}
  }

}





//ticker,open,close,adj_close,low,high,volume,date
case class StockPrice(
                       ticker: String,
                       open: Float,
                       close: Float,
                       adj_close: Float,
                       low: Float,
                       high: Float,
                       volume: Long,
                       ymd: String
                     )


object DateOrdering extends Ordering[StockPrice] {
  def compare(o1: StockPrice, o2: StockPrice) = {
    var dateo1 = LocalDate.parse(o1.ymd)
    var dateo2 = LocalDate.parse(o2.ymd)
    if (dateo1 == dateo2)
      0
    else if (dateo1.isBefore(dateo2))
      -1
    else
      1

  }
}




