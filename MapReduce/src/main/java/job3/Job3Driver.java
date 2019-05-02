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

import job1.Stock;

import org.apache.hadoop.mapreduce.Job;


public class Job3Driver extends Configured implements Tool {

	

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: Job3Driver <input path> <input path> <output path>");
			System.exit(-1);
		}
		ToolRunner.run(new Job3Driver(), args);
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
		job2.setMapOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/final"));

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.waitForCompletion(true);

		return 0;
	}



}
