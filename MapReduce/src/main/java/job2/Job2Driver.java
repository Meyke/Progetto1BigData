package job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import job1.TopNPriceChanges;

public class Job2Driver extends Configured implements Tool {
	
	static final Logger mylogger = Logger.getLogger(Job2Driver.class);
	
	public static void main(String[] args) throws Exception {
		
		long startTime = System.currentTimeMillis();
		mylogger.setLevel(Level.INFO);
		appenderLogger();
		ToolRunner.run(new Job2Driver(), args);
		mylogger.info("TEMPO DI ESECUZIONE JOB1 " + (System.currentTimeMillis() - startTime) / 1000.0 + " secondi");
	
	}
	
		 
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf);

		job1.setJarByClass(Job2Driver.class);
//		job.setMapperClass(Job2Mapper1.class);

		job1.setReducerClass(ReducerJoin.class);
		
		MultipleInputs.addInputPath(job1, new Path(args[0]),TextInputFormat.class, Job2Mapper1.class); 
		MultipleInputs.addInputPath(job1, new Path(args[1]),TextInputFormat.class, Job2Mapper2.class);
		
//		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputPath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job1, outputPath);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.waitForCompletion(true);
		
		conf = new Configuration();
		Job job2 = Job.getInstance(conf);

		job2.setJarByClass(Job2Driver.class);
		job2.setMapperClass(Job2Mapper3.class);
		job2.setReducerClass(Reducer2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/intermediate"));

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.waitForCompletion(true);
		
		conf = new Configuration();
		Job job3 = Job.getInstance(conf);

		job3.setJarByClass(Job2Driver.class);
		job3.setMapperClass(Job2Mapper4.class);
		job3.setReducerClass(FinalReducer.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job3, new Path(args[2] + "/intermediate"));
		FileOutputFormat.setOutputPath(job3, new Path(args[2] + "/final"));

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		job3.waitForCompletion(true);

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
		mylogger.info("---------INIZIO JOB2 HADOOP-----------");
		
	}

}
