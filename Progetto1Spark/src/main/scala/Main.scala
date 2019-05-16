import job3.ThreeYearTrend

object Main {

  def main(args: Array[String]): Unit = {

    //TenBestAction9818.loadData().take(3).foreach(x => println(x.ticker))
    //TenBestAction9818.topTenStocks().foreach(println)

    /*
    ThreeYearTrend.coppieAziendeSettoriDiversiTrend().take(10).foreach {
      x =>
        println(x._1._1 + " " + x._2._1 + "   " +
          x._1._2.trend2016 + "% " +
          x._1._2.trend2017 + "% " +
          x._1._2.trend2018 + " ---- " +
          x._2._2.trend2016 + "% " +
          x._2._2.trend2017 + "% " +
          x._2._2.trend2018
        )
    }*/
    //while(true){

    //}
    println("num righe = " + ThreeYearTrend.coppieAziendeSettoriDiversiTrend().take(100).foreach(println))
  }

  /*
  val conf = new SparkConf()
    .setAppName("Job1")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)

  // Load the data and parse it into a StockPrice.
  // Look at case class below
  val stocks = sc.textFile("/Users/micheletedesco1/Desktop/job1/historical_stock_prices.csv")
    .filter(_.contains("SAB,")) // filtro la prima riga

  val filtrato = stocks.filter(x => x.contains("2018-")).take(1000).foreach(println)

}*/
}
