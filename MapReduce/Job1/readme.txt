
--Upload input to HDFS
hadoop fs -mkdir -p /user/input
hadoop fs -copyFromLocal Desktop/input/ /user/input

--Delete output directory
hadoop fs -rm -r /user/output

--Execute MapReduce job
hadoop jar Desktop/job1/PrimoProgettoBigData-0.0.1-SNAPSHOT.jar job1.TopNPriceChanges /user/input/miocsv.csv /user/output

--Review output (if FileSequential)
hadoop fs -ls /user/output
hadoop fs -libjars Desktop/job1/PrimoProgettoBigData-0.0.1-SNAPSHOT.jar -text /user/output/part-r-00000
