package job3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.google.common.collect.Lists;


//DA TERMINARE
public class GenerateCoupleReducer2 extends Reducer<StockTrend, Text, Text, Text>{

	@Override
	public void reduce(StockTrend key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		ArrayList<Text> compagnie = new ArrayList<Text>();
		values.forEach( x -> compagnie.add(new Text(x.toString())));
		
		Text trendTriennio = new Text("2016:" + key.getTrend2016() +"% , " + "2017:" + key.getTrend2017() +"% , " + "2018:" + key.getTrend2018() +"%");
		

		System.out.println("TREND TRIENNIO = " + trendTriennio.toString());
		
		for (int i = 0; i< compagnie.size(); i++) {
			for (int j = i+1; j<compagnie.size(); j++) {
				context.write(new Text(compagnie.get(i) + "," + compagnie.get(j)), trendTriennio);
				System.out.println("COMPAGNIE " + new Text(compagnie.get(i) + "," + compagnie.get(j)).toString());
				System.out.println("------------");
			}
		}

	}

}
