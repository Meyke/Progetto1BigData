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

public class Job2Driver extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		
		if (args.length != 3) {
			System.err.println("Usage: Job2Driver <input path> <input path> <output path>");
			System.exit(-1);
		}
		ToolRunner.run(new Job2Driver(), args);
	
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
		job2.setReducerClass(FinalReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/final"));

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.waitForCompletion(true);

		return 0;
	}

}
