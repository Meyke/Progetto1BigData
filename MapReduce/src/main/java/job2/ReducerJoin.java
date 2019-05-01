package job2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import job1.ComparatorePerData;
import job1.Stock;
import job1.StockToOutput;

// ticker,name,sector
// AHH,"ARMADA HOFFLER PROPERTIES, INC.",FINANCE
// 
// ticker,close,date (giornalieri)
// AHH,11.5500001907349,2013-05-09,stockprice
// AHH,11.6000003814697,2013-05-10,stockprice
// AHH,11.6499996185303,2013-05-13,stockprice
// AHH,11.5299997329712,2013-05-14,stockprice

public class ReducerJoin extends Reducer<Text, Text, Text, StockTrend> {

	// attenzione a non inizializzarli qui, ma dentro il metodo reduce. Infatti l'oggetto reducer viene creato SOLO all'inizio del task
	// mentre il metodo reduce viene invocato per ogni riga in input del reducer.
	private ArrayList<Stock> quotazioni2016;
	private ArrayList<Stock> quotazioni2017;
	private ArrayList<Stock> quotazioni2018;
	 
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {


		this.quotazioni2016 = new ArrayList<Stock>();
		this.quotazioni2017 = new ArrayList<Stock>();
		this.quotazioni2018 = new ArrayList<Stock>();

		Text companyName = null;
		Text sector = null;

		//Iterate all temperatures for a year and calculate maximum
		Stock stock = null;
		
		// elimina
		//int size = 0;
		
		for (Text value : values) {
			//size ++;
			String line = value.toString();
			String[] items = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

			if (items[0].contains("stockprice")) {
				stock = creaStock(items);
				aggiungiAllalistaquotazioni(stock);		
			}
			else {		
				companyName = new Text(items[2]); //ho inserito in seconda posizione il nome della compagnia
				sector = new Text(items[3]);
			}
		}
		
		System.out.println("___________");
		System.out.println("COMPANY NAME" + companyName);
		System.out.println("TICKER " + key.toString());
		System.out.println("___________");
		
		List<Integer> trendTriennio = trendStock3years(quotazioni2016, quotazioni2017, quotazioni2018);

		//Write output
		StockTrend stocktrend = new StockTrend(key, new IntWritable(trendTriennio.get(0)), new IntWritable(trendTriennio.get(1)), new IntWritable(trendTriennio.get(2)), companyName, sector);
		context.write(companyName, stocktrend);
	}


	private void aggiungiAllalistaquotazioni(Stock stock) {
		String date = stock.getData().toString();
		if (date.contains("2016"))
			this.quotazioni2016.add(stock);
		else if (date.contains("2017"))
			this.quotazioni2017.add(stock);
		else
			this.quotazioni2018.add(stock);

	}


	private Stock creaStock(String[] items) {
		Stock stock = new Stock(new Text(items[1]), new FloatWritable(Float.parseFloat(items[2])), new FloatWritable(), new FloatWritable(), new IntWritable(), new Text(items[3]));
		return stock;
	}

	private List<Integer> trendStock3years(ArrayList<Stock> quotazioni2016, ArrayList<Stock> quotazioni2017, ArrayList<Stock> quotazioni2018) {

		ComparatorePerData cmp = new ComparatorePerData();

		Collections.sort(quotazioni2016, cmp);
		Collections.sort(quotazioni2017, cmp);
		Collections.sort(quotazioni2018, cmp);

		// determino i vari trend annuali per quell'azione, arrotondato all'intero più vicino
		//System.out.println("CALCOLO QUOTAZIONI 2016");
		Integer trend2016 = calcolaTrend(quotazioni2016);
		//System.out.println("CALCOLO QUOTAZIONI 2017");
		Integer trend2017 = calcolaTrend(quotazioni2017);
		//System.out.println("CALCOLO QUOTAZIONI 2018");
		Integer trend2018 = calcolaTrend(quotazioni2018);

		List<Integer> trendTriennio = new ArrayList<Integer>();
		Collections.addAll(trendTriennio, trend2016, trend2017, trend2018);

		return trendTriennio;

	}


	private int calcolaTrend(ArrayList<Stock> quotazioni) {

		if (quotazioni.size() == 0) //vuol dire che mancano le quotazioni per quell'anno
			return 0;
		
		float quotazioneInizioAnno = quotazioni.get(0).getClose().get();
		float quotazioneFineAnno = quotazioni.get(quotazioni.size()-1).getClose().get();
		float variazioneAnnualePerc = (quotazioneFineAnno-quotazioneInizioAnno)/quotazioneInizioAnno * 100;
		
		return Math.round(variazioneAnnualePerc);
	}




}
