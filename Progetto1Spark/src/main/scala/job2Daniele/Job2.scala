package job2Daniele

import java.util.{Calendar, GregorianCalendar}

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

import logger.MyLogger

object Job2 {

  var pathToFile1 = ""
  var pathToFile2 = ""
  var outputPath = ""

  var sc: SparkContext = null


  def loadData1(): RDD[(String, String)] = {

    sc.textFile(pathToFile1)
      .filter(line => getYearByDate(line.split(",")(7)) > 2003)
      .map(line => (line.split(",")(0), line.split(",")(2)
        + "," + line.split(",")(6) + "," + line.split(",")(7)))
  }

  def loadData2(): RDD[(String, String)] = {

    sc.textFile(pathToFile2)
      .map(line => (splitIngoringCommas(line)(0), splitIngoringCommas(line)(3)))
  }




  def splitIngoringCommas(line: String): Array[String] = {
    line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
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
    val filter_result = loadData1().partitionBy(new HashPartitioner(8))
    filter_result.map(line => (line._1 + "," + getYearByDate(line._2.split(",")(2))
      , line._2.split(",")(0) + "," + line._2.split(",")(1)
      + "," + line._2.split(",")(2))).groupByKey()
  }

  def reduceByTickerAndYear(): RDD[(String,String)] = {
    val grouped_result = groupByTickerAndYear()
    grouped_result.map(line => (line._1, calculateValuesByTickerAndYear(line._2.toList)))
  }

  def dataJoin(): RDD[(String,(String, String))] = {
    val reduced_result = reduceByTickerAndYear().map(line => (line._1.split(",")(0),
      line._1.split(",")(1) + "," + line._2))
    val historical_stocks = loadData2().partitionBy(new HashPartitioner(8))
    reduced_result.join(historical_stocks)
  }

  def groupBySectorAndYear(): RDD[(String,Iterable[String])] = {
    val join_result = dataJoin()
    join_result.map(line => (line._2._2 + "," + line._2._1.split(",")(0),
      line._2._1.split(",")(1) + "," + line._2._1.split(",")(2) + ","
        + line._2._1.split(",")(3))).groupByKey()
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

    "%1.2f".format(quotazione) + "," + volume.toString()  + "," + "%1.2f".format(variazione_percentuale) + "%"
  }



  def main(args: Array[String]): Unit = {

    var startTime = System.currentTimeMillis()

    val log = new MyLogger(this.getClass, 2)
    log.appenderLogger()


    pathToFile1 = args(0)
    pathToFile2 = args(1)
    outputPath = args(2)

    val conf = new SparkConf()
      .setAppName("Job2")
    //.setMaster("local[*]") // here local mode. And * means you will use as much as you have cores.

    sc = new SparkContext(conf)

    Job2.reduceBySectorAndYear().coalesce(1).saveAsTextFile(outputPath)

    log.timeLog((System.currentTimeMillis() - startTime) / 1000.0)
  }
}
