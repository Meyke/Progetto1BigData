package job2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FinalReducer extends Reducer<Text, Text, Text, Text> {
	
	private final int CLOSE = 1;
	private final int VOLUME = 2;
	
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		long volume = 0;
		double quotazione = 0;
		for(Text value : values) {
			String[] parts = value.toString().split(",");
			volume += Long.parseLong(parts[VOLUME]);
			quotazione += Double.parseDouble(parts[CLOSE]);
		}
		context.write(key, new Text(String.valueOf(volume) + "," + String.valueOf(quotazione)));
	}

}
