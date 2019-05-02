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
 * Tale classe ha lo scopo di leggere il dataset historical_stock
 * ed estrarre le informazioni necessarie, quali ticker, companyname, sector 
 * @author micheletedesco1
 *
 */
public class MapperHistoricalStock extends Mapper<LongWritable, Text, Text, Text>{

	// ticker,exchange,name,sector,industry
	private final int TICKER = 0;
	private final int NAME = 2;
	private final int SECTOR = 3;     



	/**
	 * Map method. The result is <ticker, information about that Companystock>
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (value.toString().contains("ticker,exchange,name,sector,industry")) return; //per rimuovere l'header

		String[] items = estraiInformazioniDalText(value.toString());

		//extract various field of csv in writable format for passing to reducer

		Text ticker = new Text(items[TICKER]);
		Text companyName = new Text(items[NAME]);
		Text sector = new Text(items[SECTOR]);


		Text companystock = new Text("companyStock" + "," + ticker + "," + companyName + "," + sector);
		context.write(new Text(ticker), companystock);



	}



	/*
	 * Splitting on comma outside quotes [duplicate]
	 * Metodo preso da https://stackoverflow.com/questions/18893390/splitting-on-comma-outside-quotes
	 */
	private String[] estraiInformazioniDalText(String str) {

		String[] array = str.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
		return array;
	}

}
