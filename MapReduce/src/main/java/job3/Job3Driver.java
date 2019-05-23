package job3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import job1.Stock;
import job1.TopNPriceChanges;

import org.apache.hadoop.mapreduce.Job;


public class Job3Driver extends Configured implements Tool {

	static final Logger mylogger = Logger.getLogger(Job3Driver.class);

	public static void main(String[] args) throws Exception {
		//Logger per registrare i tempi di esecuzione del job
		long startTime = System.currentTimeMillis();
		mylogger.setLevel(Level.INFO);
		appenderLogger();
		//if (args.length != 3) {
		//	System.err.println("Usage: Job3Driver <input path> <input path> <output path>");
		//	System.exit(-1);
		//}

		ToolRunner.run(new Job3Driver(), args);
		
		mylogger.info("TEMPO DI ESECUZIONE JOB1 " + (System.currentTimeMillis() - startTime) / 1000.0 + " secondi");
	}


	public int run(String[] args) throws Exception {


		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf);

		job1.setJarByClass(Job3Driver.class);
		job1.setJobName("JOB_1");

		MultipleInputs.addInputPath(job1, new Path(args[0]),TextInputFormat.class, MapperHistoricalStockPrice.class);
		MultipleInputs.addInputPath(job1, new Path(args[1]),TextInputFormat.class, MapperHistoricalStock.class);

		job1.setReducerClass(ReducerJoin.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));


		job1.waitForCompletion(true);

		conf = new Configuration(); //importantissimo reinizializzarla
		Job job2 = Job.getInstance(conf);

		job2.setJarByClass(Job3Driver.class);
		job2.setMapperClass(GenerateCoupleMapper2.class);
		job2.setReducerClass(GenerateCoupleReducer2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(StockTrend.class);

		FileInputFormat.addInputPath(job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/final"));

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.waitForCompletion(true);

		return 0;
	}
	
	private static void appenderLogger() {
		FileAppender fa = new FileAppender();
		fa.setName("FileLogger"); //nome appender
		fa.setFile("mylog.log"); // nome file
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
		fa.setThreshold(Level.INFO);
		fa.setAppend(true);
		fa.activateOptions();
		Logger.getRootLogger().addAppender(fa);
		mylogger.info("---------INIZIO JOB3 HADOOP-----------");
		
	}



}
