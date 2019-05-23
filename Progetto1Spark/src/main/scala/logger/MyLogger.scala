package logger

import org.apache.log4j.FileAppender
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.PatternLayout

class MyLogger(var c: Class[_], var numJob: Int) {

  val mylogger = Logger.getLogger(c)

  def appenderLogger() : Unit = {

    mylogger.setLevel(Level.INFO);
    var fa = new FileAppender();
    fa.setName("FileLogger"); //nome appender
    fa.setFile("mylog.log"); // nome file
    fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
    fa.setThreshold(Level.INFO);
    fa.setAppend(true);
    fa.activateOptions();
    Logger.getRootLogger().addAppender(fa);
    mylogger.info("---------INIZIO JOB " + numJob + " SPARK-----------");

  }

  def timeLog (time: Double): Unit= {
    mylogger.info("TEMPO DI ESECUZIONE JOB1 " + time + " secondi")

  }




}