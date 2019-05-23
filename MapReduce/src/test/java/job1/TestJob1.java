package job1;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class TestJob1 {

	@Test
	public void testMapper() throws Exception {

		MapDriver <LongWritable, Text, Text, Stock> mp = new MapDriver<LongWritable, Text, Text, Stock>();
		mp = mp.withMapper(new TopNStocksMapper());
		mp.withInput(new LongWritable(1), new Text("AHH,11.5,11.5799999237061,8.49315452575684,11.25,11.6800003051758,4633900,2018-05-08"));
		//mp.withInput(new LongWritable(1), new Text("ticker,open,close,adj_close,low,high,volume,date"));
		System.out.println(mp.run().toString());

	}

	@Test
	public void testReducer() throws Exception {

		List<Stock> values = new ArrayList<Stock>();


		/*
		 * 
	           AHH,11.5799999237061,11.25,11.6800003051758,4633900,2013-05-08
     		   AHH,11.5500001907349,11.5,11.6599998474121,275800,2013-05-09
			   AHH,11.6000003814697,11.5,11.6000003814697,277100,2013-05-10
		 */
	
		Stock stock1 = new Stock(new Text("AHH"), new FloatWritable((float) 11.6000003814697), new FloatWritable((float) 11.5), 
				new FloatWritable((float) 11.6000003814697), new LongWritable(277100), new Text("2013-05-10"));
		
		Stock stock2 = new Stock(new Text("AHH"), new FloatWritable((float) 11.5799999237061), new FloatWritable((float) 11.25), 
				new FloatWritable((float) 11.6800003051758), new LongWritable(4633900), new Text("2013-05-08"));

		Stock stock3 = new Stock(new Text("AHH"), new FloatWritable((float) 11.5500001907349), new FloatWritable((float) 11.5), 
				new FloatWritable((float) 11.6599998474121), new LongWritable(275800), new Text("2013-05-09"));


		values.add(stock1);
		values.add(stock2);
		values.add(stock3);


		ReduceDriver <Text, Stock, Text, StockToOutput> rd = new ReduceDriver<Text, Stock, Text, StockToOutput>();
		rd = rd.withReducer(new TopNStocksReducer()); //Sets the reducer object to use for this test
		rd.withInput(new Text("AHH"), values); // imposta l'imput da inviare al reducer setInput(K1 key, List<V1> values)
		System.out.println(rd.run().toString());

	}

	
}
