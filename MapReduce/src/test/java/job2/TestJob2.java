package job2;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import job1.Stock;
import job1.StockToOutput;


public class TestJob2 {

	
	@Test
	public void testMapperHistoricalStockPrice() throws Exception {

		MapDriver <LongWritable, Text, Text, Text> mp = new MapDriver<LongWritable, Text, Text, Text>();
		mp = mp.withMapper(new MapperHistoricalStockPrice());
		//mp.withInput(new LongWritable(1), new Text("AHH,11.5,11.5799999237061,8.49315452575684,11.25,11.6800003051758,4633900,2013-05-08"));
		//mp.withInput(new LongWritable(1), new Text("ticker,open,close,adj_close,low,high,volume,date"));
		mp.withInput(new LongWritable(1), new Text("AHH,11.5,11.5799999237061,8.49315452575684,11.25,11.6800003051758,4633900,2017-05-08"));
		System.out.println(mp.run().toString());
		System.out.println("\n");

	}


	@Test
	public void testMapperHistoricalStock() throws Exception {

		MapDriver <LongWritable, Text, Text, Text> mp = new MapDriver<LongWritable, Text, Text, Text>();
		mp = mp.withMapper(new MapperHistoricalStock());
		//mp.withInput(new LongWritable(1), new Text("AHH,11.5,11.5799999237061,8.49315452575684,11.25,11.6800003051758,4633900,2013-05-08"));
		//mp.withInput(new LongWritable(1), new Text("ticker,open,close,adj_close,low,high,volume,date"));
		mp.withInput(new LongWritable(1), new Text("PIHPP,NASDAQ,\"1347 PROPERTY INSURANCE HOLDINGS, INC.\",FINANCE,PROPERTY-CASUALTY INSURERS"));
		System.out.println(mp.run().toString());
		System.out.println("\n");

	}

	

	@Test
	public void testReducer() throws Exception {

		/*
		 *    WAT,NYSE,WATERS CORPORATION,CAPITAL GOODS,BIOTECHNOLOGY: LABORATORY ANALYTICAL INSTRUMENTS
		 * 
		 */
		
		/*
		 * 
           
           WAT,195.690002441406,2018-01-02
           WAT,197.770004272461,2018-01-03
           WAT,199.660003662109,2018-01-04
           WAT,202.229995727539,2018-01-05
           WAT,204.759994506836,2018-01-08
           WAT,208.960006713867,2018-01-09
           WAT,207.320007324219,2018-01-10
           WAT,207.729995727539,2018-01-11
           WAT,209.75,2018-01-12
           WAT,208.940002441406,2018-01-16
           WAT,209.160003662109,2018-01-17
           WAT,207.869995117188,2018-01-18
           WAT,210.720001220703,2018-01-19
           WAT,214.770004272461,2018-01-22
           WAT,210.990005493164,2018-01-23
           WAT,214.300003051758,2018-01-24
           WAT,217.210006713867,2018-01-25
           WAT,218.139999389648,2018-01-26
           WAT,218.699996948242,2018-01-29
           WAT,214.869995117188,2018-01-30
           WAT,215.610000610352,2018-01-31
		 */


		Text stock1 = new Text("stockprice,WAT,12.5600709915161,2016-08-04");
		Text stock2 = new Text("stockprice,WAT,12.5062408447266,2016-08-05");
		Text stock3 = new Text("stockprice,WAT,13.8699998855591,2016-08-08");
		Text stock4 = new Text("stockprice,WAT,14.1499996185303,2016-08-09");
		Text stock5 = new Text("stockprice,WAT,14.0299997329712,2016-08-10");
		Text stock6 = new Text("stockprice,WAT,15.5799999237061,2017-12-14");
		Text stock7 = new Text("stockprice,WAT,15.6999998092651,2017-12-15");
		Text stock8 = new Text("stockprice,WAT,15.8500003814697,2017-12-18");
		Text stock9 = new Text("stockprice,WAT,15.4700002670288,2017-12-19");
		Text stock10 = new Text("stockprice,WAT,15.4899997711182,2017-12-20");
		
		Text company = new Text("companyStock,WAT,WATERS CORPORATION,CAPITAL GOODS"); //aggiustare mapper per nome di certe compagnie
		
		Text stock11 = new Text("stockprice,WAT,195.690002441406,2018-01-02");
		Text stock12 = new Text("stockprice,WAT,197.770004272461,2018-01-03");
		Text stock13 = new Text("stockprice,WAT,199.660003662109,2018-01-04");
		Text stock14 = new Text("stockprice,WAT,202.229995727539,2018-01-05");
		Text stock15 = new Text("stockprice,WAT,204.759994506836,2018-01-08");
		Text stock16 = new Text("stockprice,WAT,208.960006713867,2018-01-09");
		Text stock17 = new Text("stockprice,WAT,207.320007324219,2018-01-10");
		Text stock18 = new Text("stockprice,WAT,209.75,2018-01-12");
		Text stock19 = new Text("stockprice,WAT,208.940002441406,2018-01-16");
		Text stock20 = new Text("stockprice,WAT,209.160003662109,2018-01-17");
		Text stock21 = new Text("stockprice,WAT,207.869995117188,2018-01-18");
		Text stock22 = new Text("stockprice,WAT,210.720001220703,2018-01-19");
		Text stock23 = new Text("stockprice,WAT,214.770004272461,2018-01-22");
		Text stock24 = new Text("stockprice,WAT,210.990005493164,2018-01-23");
		Text stock25 = new Text("stockprice,WAT,214.300003051758,2018-01-24");
		Text stock26 = new Text("stockprice,WAT,217.210006713867,2018-01-25");
		Text stock27 = new Text("stockprice,WAT,218.139999389648,2018-01-26");
		Text stock28 = new Text("stockprice,WAT,218.699996948242,2018-01-29");
		Text stock29 = new Text("stockprice,WAT,214.869995117188,2018-01-30");
		Text stock30 = new Text("stockprice,WAT,215.610000610352,2018-01-31");
		

		List<Text> values = new ArrayList<Text>();
		Collections.addAll(values, stock1, stock2, stock3, stock4, stock5, stock6, stock7, stock8, stock9, stock10, company, 
				stock11, stock12,stock21,stock22,stock23,stock24,
				stock25,stock26,stock27,stock28,stock29,stock30, stock13, stock14, stock15,stock16,stock17,stock18,stock19,stock20);


		ReduceDriver <Text, Text, Text, StockTrend> rd = new ReduceDriver<Text, Text, Text, StockTrend>();
		rd = rd.withReducer(new ReducerJoin()); //Sets the reducer object to use for this test
		rd.withInput(new Text("WAT"), values); // imposta l'imput da inviare al reducer setInput(K1 key, List<V1> values)
		System.out.println(rd.run().toString());

	}




}
