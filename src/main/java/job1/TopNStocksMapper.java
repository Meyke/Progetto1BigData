package job1;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TopNStocksMapper extends Mapper<LongWritable, Text, Text, Stock> {
	
	// ticker,open,close,adj_close,low,high,volume,date
	private final int TICKER = 0;
	private final int CLOSE = 2;
	private final int LOW = 4;
	private final int HIGH = 5;
	private final int VOLUME = 6;
	private final int DATE = 7;
	
	
	/**
	 * Map method. The result is <ticker, information about that stock>
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] items = line.split(",");
		
		//extract various field of csv in writable format for passing to reducer

		Text ticker = new Text(items[TICKER]);
		FloatWritable close = new FloatWritable(Float.parseFloat(items[CLOSE]));
		FloatWritable low = new FloatWritable(Float.parseFloat(items[LOW]));
		FloatWritable high = new FloatWritable(Float.parseFloat(items[HIGH]));
		IntWritable volume = new IntWritable(Integer.parseInt(items[VOLUME]));
		Text date = new Text(items[DATE]);

		Stock stock = new Stock(ticker, close, low, high, volume, date);
		context.write(new Text(ticker), stock);

	}


}
