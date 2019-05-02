package job3;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


//DA TERMINARE
public class GenerateCoupleReducer2 extends Reducer<StockTrend, Text, Text, Text>{

	 @Override
	 public void reduce(StockTrend key, Iterable<Text> values, Context context)
			 throws IOException, InterruptedException {
		 //dovrei avere il trend e una lista di compagnie aventi stesso trend
		 context.write(new Text("A"), new Text("B"));
		 
	 }

}
