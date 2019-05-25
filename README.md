# Progetto 1 corso di BigData

Il seguente progetto ha lo scopo di mettere a confronto 3 tecnologie per la gestione dei Big Data, quali Hadoop MapReduce, Hive, Spark.

### Organizzazione repository

Questa repository è organizzata in 3 cartelle:
* [MapReduce](https://github.com/Meyke/Progetto1BigData/tree/master/MapReduce);
* [Hive](https://github.com/Meyke/Progetto1BigData/tree/master/hive);
* [Spark](https://github.com/Meyke/Progetto1BigData/tree/master/Progetto1Spark);

In ogni cartella è presente il codice sorgente per ognuno dei Job.

Inoltre, è presente la cartella [jarsForQuickExecution](https://github.com/Meyke/Progetto1BigData/tree/master/jarsForQuickExecution), in cui sono presenti i jars dei progetti per Hadoop e Spark, e un file di testo che indica come eseguire tali jars.


### Installing

Se si vogliono eseguire i job in locale è necessario installarsi la Java JDK 8 o successive, Apache Hadoop (3.1.2 o successive), Apache Hive (2.3.4 o successive), Apache Spark (2.4.0 o successive) 


### Tests

Sono stati eseguiti test di efficienza per ognuno dei job, confrontando i tempi di esecuzione in locale rispetto ai tempi di esecuzione su cluster, con dimensioni del dataset e del cluster via via crescenti.


## Built With

* [Maven](https://maven.apache.org/) - Dependency Management
* [Sbt](https://www.scala-sbt.org) - The interactive build tool

## Authors

<p align="center">
<img src="images/logo.png" width="500" height="650">
</p>

che poi sarebbero **Michele Tedesco** e **Daniele Caldarini**

## Bibliografia

Slides del [corso](http://torlone.dia.uniroma3.it/bigdata/materiale.html)












