package job1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class TopNStocksReducer extends Reducer<Text, Stock, Text, StockToOutput> {

	@Override
	public void reduce(Text key, Iterable<Stock> values, Context context)
			throws IOException, InterruptedException {

		ArrayList<Stock> stockOrdinatiPerData = stockOrdinatoPerData(values);
		Float[] percentage = percentChangeCalculation(stockOrdinatiPerData);
		Float overallpercChange = overallPercChange(percentage);
		Float maxClosePrice = calcoloMaxPrice(stockOrdinatiPerData);
		Float minClosePrice = calcoloMinPrice(stockOrdinatiPerData);
		Float meanvolumedaily = calcoloVolumeMedioGiornaliero(stockOrdinatiPerData);
		//Write output
		StockToOutput stock = new StockToOutput(key, new FloatWritable(overallpercChange), new FloatWritable(minClosePrice), new FloatWritable(maxClosePrice), new FloatWritable(meanvolumedaily));
		context.write(key, stock);
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
	 * Tale metodo serve a calcolare come è incrementato (o diminuito) l'incremento percentuale avendo diversi cambi percentuali
	 * @return
	 * https://gmatclub.com/forum/excellent-method-for-calculating-successive-percentages-185973.html
	 */
	private Float overallPercChange(Float[] percentage) {
		int N = percentage.length; 
		float var1, var2, result = 0; 


		var1 = percentage[0]; 
		var2 = percentage[1]; 

		// Calculate successive change of 1st 2 change 
		result = var1 + var2 + ((var1 * var2) / 100); 

		// Calculate successive change 
		// for rest of the value 
		for (int i = 2; i < N; i++) 
			result = result + percentage[i] + ((result * percentage[i]) / 100); 

		return result; 
	}

	/**
	 * Tale metodo serve a calcolare le variazioni percentuali giornaliere. Restituisce un array di variazioni percentuali per quello stock
	 * Daily Chg = (Price - Prev Day's Close) / Prev Day's Close * 100
	 * @param stockOrdinatiPerData
	 * https://www.stockcharts.com/docs/doku.php?id=faqs:how_is_percent_change_calulated
	 */
	private Float[] percentChangeCalculation(ArrayList<Stock> stockOrdinatiPerData) {
		Float[] percentage = new Float[stockOrdinatiPerData.size()-1];
		for(int i = 1; i < stockOrdinatiPerData.size(); i++ ){ 
			Float prezzoGiornoX = stockOrdinatiPerData.get(i).getClose().get();
			Float prezzoGiornoPrecX = stockOrdinatiPerData.get(i-1).getClose().get();
			percentage[i-1] = ((prezzoGiornoX - prezzoGiornoPrecX) / prezzoGiornoPrecX) *100;
		}
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
