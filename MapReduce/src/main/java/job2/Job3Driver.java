package job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import job1.Stock;

import org.apache.hadoop.mapreduce.Job;


public class Job3Driver {

	public static void main(String[] args) throws Exception {

		
		if (args.length != 3) {
			System.err.println("Usage: Job3Driver <input path> <input path> <output path>");
			System.exit(-1);
		}
		
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Job3Driver.class);
		job.setJobName("Terzo job");
		
		job.setReducerClass(ReducerJoin.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StockTrend.class);

		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, MapperHistoricalStockPrice.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, MapperHistoricalStock.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		//job.setNumReduceTasks(3);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
