package job1;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

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

		if (value.toString().contains("ticker,open,close,adj_close,low,high,volume,date")) return; //per rimuovere l'header

		String line = value.toString();
		String[] items = line.split(",");

		//extract various field of csv in writable format for passing to reducer

		Text ticker = new Text(items[TICKER]);
		FloatWritable close = new FloatWritable(Float.parseFloat(items[CLOSE]));
		FloatWritable low = new FloatWritable(Float.parseFloat(items[LOW]));
		FloatWritable high = new FloatWritable(Float.parseFloat(items[HIGH]));
		LongWritable volume = new LongWritable(Long.parseLong(items[VOLUME]));
		Text date = new Text(items[DATE]);

		try {
			int year = getYearFromDate(items[DATE]);
			if (year >= 1998 && year <= 2018) {
				Stock stock = new Stock(ticker, close, low, high, volume, date);
				context.write(new Text(ticker), stock);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}

	}


	private int getYearFromDate(String string) throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date date = format.parse(string.trim());

		Calendar c = new GregorianCalendar();
		c.setTime(date);
		int year = c.get(Calendar.YEAR);	
		return year;
	}


}
