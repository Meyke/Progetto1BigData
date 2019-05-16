package job2Daniele

import java.util.{Calendar, GregorianCalendar}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

object Job2 {
  //  val pathToFile1 = "hdfs://localhost:9000/user/daniele/input/historical_stock_prices.csv"
  //  val pathToFile2 = "hdfs://localhost:9000/user/daniele/input/historical_stocks.csv"
  var pathToFile1 = "/Users/micheletedesco1/Desktop/job1/historical_stock_prices.csv"
  var pathToFile2 = "/Users/micheletedesco1/Desktop/job1/historical_stocks.csv"
  //var outputPath = "file3"
  /**
    *  Load the data from the text files and join these
    */

  def loadDataAndJoin(): RDD[(String,(String, String))] = {
    // create spark configuration and spark context: the Spark context is the entry point in Spark.
    // It represents the connexion to Spark and it is the place where you can configure the common properties
    // like the app name, the master url, memories allocation...
    val conf = new SparkConf()
      .setAppName("Job2")
      .setMaster("local[*]") // here local mode. And * means you will use as much as you have cores.

    val sc = new SparkContext(conf)

    val historical_stock_prices = sc.textFile(pathToFile1)
      .map(line => (splitIngoringCommas(line)(0), splitIngoringCommas(line)(2)
        + "," + splitIngoringCommas(line)(6) + "," + splitIngoringCommas(line)(7)))

    val historical_stock = sc.textFile(pathToFile2)
      .map(line => (splitIngoringCommas(line)(0), splitIngoringCommas(line)(3)))


    return historical_stock_prices.join(historical_stock)
  }

  def splitIngoringCommas(line: String): Array[String] = {
    line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
  }

  def filterByYear(): RDD[(String,(String, String))] = {
    val join_result = loadDataAndJoin()

    join_result.filter(line => getYearByDate(splitIngoringCommas(line._2._1)(2)) > 2003)
  }

  def getYearByDate(date: String): Int = {
    try {
      val cal = new GregorianCalendar()
      val date_parsed = parseToDate(date)
      cal.setTime(date_parsed)
      cal.get(Calendar.YEAR)
    }
    catch  {
      case ex: ParseException => {
        return 1000
      }
    }
  }

  def parseToDate(date: String): Date = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    format.parse(date)
  }

  def getDateFromString(line: String): Date = {
    val date = line.split(",")(2)
    parseToDate(date)
  }

  def compare(a:String, b:String): Boolean = {
    if (getDateFromString(a).compareTo(getDateFromString(b)) < 0) {
      return true
    }
    else {
      return false
    }
  }

  def groupByTickerAndYear(): RDD[(String,Iterable[String])] = {
    val filter_result = filterByYear()
    filter_result.map(line => (line._1 + "," + getYearByDate(line._2._1.split(",")(2))
      + "," + line._2._2, line._2._1.split(",")(0) + "," + line._2._1.split(",")(1)
      + "," + line._2._1.split(",")(2))).groupByKey()
  }

  def reduceByTickerAndYear(): RDD[(String,String)] = {
    val grouped_result = groupByTickerAndYear()
    grouped_result.map(line => (line._1, calculateValuesByTickerAndYear(line._2.toList)))
  }

  def groupBySectorAndYear(): RDD[(String,Iterable[String])] = {
    val filter_result = reduceByTickerAndYear()
    filter_result.map(line => (line._1.split(",")(2) + "," + line._1.split(",")(1),
      line._2.split(",")(0) + "," + line._2.split(",")(1) + "," + line._2.split(",")(2))).groupByKey()
  }

  def reduceBySectorAndYear(): RDD[(String,String)] = {
    val grouped_result = groupBySectorAndYear()
    grouped_result.map(line => (line._1, aggregateValues(line._2.toList)))
  }

  def calculateValuesByTickerAndYear(lista: List[String]): String = {
    var quotazione = 0.0
    var volume = 0.0
    val lista_ordinata = lista.sortWith((a,b) => compare(a,b))
    val quotazione_inizio_anno = lista_ordinata.head.split(",")(0).toDouble
    val quotazione_fine_anno = lista_ordinata.last.split(",")(0).toDouble
    for(x <- lista_ordinata ){
      var y = x.split(",")
      quotazione += y(0).toDouble
      volume += y(1).toDouble
    }
    val variazione_percentuale = ((quotazione_fine_anno - quotazione_inizio_anno)/quotazione_inizio_anno)*100;

    quotazione.toString + "," + volume.toString()  + "," + variazione_percentuale.toString()
  }

  def aggregateValues(lista: List[String]): String = {
    var quotazione = 0.0
    var volume = 0.0
    var variazione_percentuale = 0.0
    for(x <- lista){
      var y = x.split(",")
      quotazione += y(0).toDouble
      volume += y(1).toDouble
      variazione_percentuale += y(2).toDouble
    }

    "%1.2f".format(quotazione) + "," + volume.toString()  + "," + "%1.2f".format(variazione_percentuale)
  }



  def main(args: Array[String]): Unit = {


    //outputPath = args(2)

    Job2.reduceBySectorAndYear().take(10).foreach(println)
  }
}
