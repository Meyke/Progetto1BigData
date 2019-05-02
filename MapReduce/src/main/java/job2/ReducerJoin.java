package job2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.google.common.collect.Lists;

public class ReducerJoin extends 
	Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		Text sector = null;
		List<Text> list = new ArrayList<>();
		for (Text value : values) {
			list.add(new Text(value.toString()));
		}
		for (Text value : list) {
			if(value.toString().contains("table2")) {
				String[] parts = value.toString().split(",");
				sector = new Text(parts[0]);
			}
		}
		for(Text value : list) {
			if(!(value.toString().contains("table2"))) {
				context.write(key, new Text(value.toString() + "," + sector.toString()));
			}
		}
	}
}
