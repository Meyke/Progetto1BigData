package job1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class TopNStocksReducer extends Reducer<Text, Stock, Text, StockToOutput> {

	private List<StockToOutput> topN = new ArrayList<>();

	@Override
	public void reduce(Text key, Iterable<Stock> values, Context context)
			throws IOException, InterruptedException {

		ArrayList<Stock> stockOrdinatiPerData = stockOrdinatoPerData(values);

		Float percentage = percentChangeCalculation(stockOrdinatiPerData);

		Float maxClosePrice = calcoloMaxPrice(stockOrdinatiPerData);
		Float minClosePrice = calcoloMinPrice(stockOrdinatiPerData);
		Float meanvolumedaily = calcoloVolumeMedioGiornaliero(stockOrdinatiPerData);
		//Write output
		StockToOutput stock = new StockToOutput(new Text(key), new FloatWritable(percentage), new FloatWritable(minClosePrice), new FloatWritable(maxClosePrice), new FloatWritable(meanvolumedaily));
		topN.add(stock);
		//context.write(key, stock);
	}

	
	/**
    cleanup viene invocato SOLO UNA VOLTA ALLA FINE DI TUTTI I JOB
	 **/
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		System.out.println("_______A_______" + topN);
		int count = 0;
		Collections.sort(topN);
		System.out.println("_______B_______" + topN);
		System.out.println("elemento: " +  count + " " + topN.get(0).toString());
		
		for(StockToOutput st: topN) {
			context.write(st.getTicker(), st);
			count ++;
			if (count == 10)
				break;
		}
		
	}
	



	private Float calcoloVolumeMedioGiornaliero(ArrayList<Stock> stockOrdinatiPerData) {

		Float volumeTotale = new Float(0);
		for (Stock value : stockOrdinatiPerData) {
			volumeTotale += (float) value.getVolume().get();
		}
		Float volumeGiornalieroMedio = volumeTotale/stockOrdinatiPerData.size();
		return volumeGiornalieroMedio;
	}


	private Float calcoloMaxPrice(ArrayList<Stock> stockOrdinatiPerData) {
		float maxClosePrice = Float.MIN_VALUE;

		//Iterate all temperatures for a year and calculate maximum
		for (Stock value : stockOrdinatiPerData) {
			maxClosePrice = Math.max(maxClosePrice, value.getClose().get()); //la funzione max per comparare due valori
		}

		return maxClosePrice;
	}

	private Float calcoloMinPrice(ArrayList<Stock> stockOrdinatiPerData) {
		float minClosePrice = Float.MAX_VALUE;

		//Iterate all temperatures for a year and calculate maximum
		for (Stock value : stockOrdinatiPerData) {
			minClosePrice = Math.min(minClosePrice, value.getClose().get()); //la funzione max per comparare due valori
		}

		return minClosePrice;
	}


	/**
	 * Tale metodo serve a calcolare le variazioni percentuali dal 1998 al 2018. 
	 * Daily Chg = (Price - Prev Day's Close) / Prev Day's Close * 100
	 * Nel caso l'azienda non fosse presente nel 1998 si prende la prima data utile. Non si considerano picchi intermedi
	 * @param stockOrdinatiPerData
	 * https://www.stockcharts.com/docs/doku.php?id=faqs:how_is_percent_change_calulated
	 */
	private Float percentChangeCalculation(ArrayList<Stock> stockOrdinatiPerData) {
		Float percentage = new Float(0);
		Float priceFirstDay1998 = stockOrdinatiPerData.get(0).getClose().get();
		Float pricelastDay2018 = stockOrdinatiPerData.get(stockOrdinatiPerData.size()-1).getClose().get();
		percentage = ((pricelastDay2018 - priceFirstDay1998) / priceFirstDay1998) *100;
		return percentage;

	}

	private ArrayList<Stock> stockOrdinatoPerData(Iterable<Stock> values) {

		ComparatorePerData cmp = new ComparatorePerData();

		ArrayList<Stock> stockOrdinatiPerData = new ArrayList<Stock>();


		for (Stock value: values) {
			Stock s = new Stock(value.getTicker(), value.getClose(), value.getLow(), value.getHigh(), value.getVolume(), value.getData());
			stockOrdinatiPerData.add(s);
		}

		Collections.sort(stockOrdinatiPerData, cmp);
		return stockOrdinatiPerData;

	}

}
