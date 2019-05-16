package job3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.google.common.collect.Lists;


//DA TERMINARE
public class GenerateCoupleReducer2 extends Reducer<Text, StockTrend, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<StockTrend> values, Context context)
			throws IOException, InterruptedException {

		ArrayList<StockTrend> compagnie = new ArrayList<StockTrend>();
		values.forEach( x -> compagnie.add(new StockTrend(x.getTicker(), x.getTrend2016(), x.getTrend2017(), x.getTrend2018(), x.getCompanyName(), x.getSector())));

		
		//Text trendTriennio = new Text("2016:" + key.getTrend2016() +"% , " + "2017:" + key.getTrend2017() +"% , " + "2018:" + key.getTrend2018() +"%");
		
		/*
		String stampa = "";
		for (int i = 0; i< compagnie.size(); i++)
			stampa = stampa + " --- " + compagnie.get(i);
		if(key.getTrend2016().get() + key.getTrend2018().get() + key.getTrend2017().get() != 0)
			context.write(new Text(stampa), trendTriennio);
		
		*/
		
		for (int i = 0; i< compagnie.size(); i++) {
			for (int j = i+1; j<compagnie.size(); j++) {
				if (compagnie.get(i).getSector().equals(compagnie.get(j).getSector()) == false)
					context.write(new Text(compagnie.get(i).getCompanyName().toString() + "," + compagnie.get(j).getCompanyName().toString()), key);
				
			}
		}

	}

}
