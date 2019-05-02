package job2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Job2Mapper2 extends Mapper<LongWritable, Text, Text, Text>{
	
	private final int TICKER = 0;
	private final int SECTOR = 3;
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] parts = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
//		String[] parts = line.split(",");
		context.write(new Text(parts[TICKER]), new Text(parts[SECTOR] + ",table2"));
	}

}