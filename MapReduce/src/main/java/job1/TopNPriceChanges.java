package job1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class TopNPriceChanges {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage: TopNPriceChanges <input path> <output path>");
			System.exit(-1);
		}

		//Define MapReduce job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(TopNPriceChanges.class);
		job.setJobName("Job1");

		//Set input and output locations
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//Set Input and Output formats
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class); // perchè andrò a scrivere un binario

		//Set Mapper and Reduce classes
		job.setMapperClass(TopNStocksMapper.class);
		job.setReducerClass(TopNStocksReducer.class);

		//Mapper-Reducer_combiner specifications
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Stock.class);

		//Output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StockToOutput.class);

		//Submit job
		System.exit(job.waitForCompletion(true) ? 0 : 1); // se true vediamo l'esecuzione del job su console
	}

}


