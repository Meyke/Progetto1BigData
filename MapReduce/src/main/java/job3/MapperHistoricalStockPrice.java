package job3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import job1.Stock;

/**
 * Tale classe ha lo scopo di leggere il dataset historical_stock_price 
 * ed estrarre le informazioni necessarie, quali ticker, close, date per
 * solo quelli che vanno nel 2016, 2017, 2018.
 * @author micheletedesco1
 *
 */
public class MapperHistoricalStockPrice  extends Mapper<LongWritable, Text, Text, Text> {
	
	// ticker,open,close,adj_close,low,high,volume,date
	private final int TICKER = 0;
	private final int CLOSE = 2;
	private final int DATE = 7;
	

	/**
	 * Map method. The result is <ticker, information about that stock>
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (value.toString().contains("ticker,open,close,adj_close,low,high,volume,date")) return; //per rimuovere l'header
		
		String line = value.toString();
		String[] items = line.split(",");
		
		//extract various field of csv in writable format for passing to reducer

		Text ticker = new Text(items[TICKER]);
		FloatWritable close = new FloatWritable(Float.parseFloat(items[CLOSE]));
		Text date = new Text(items[DATE]);
		
		// I'm only interested in the last 3 years
		
		if (isInThreeYearPeriod(date)) {
			Text stockInformation = new Text("stockprice" + "," + ticker + "," + close + "," + date);
			context.write(new Text(ticker), stockInformation);
		}

	}

	private boolean isInThreeYearPeriod(Text date) {
		String ymd[] = date.toString().trim().split("-");
		return ymd[0].equals("2016") || ymd[0].equals("2017") || ymd[0].equals("2018");
	}

}
