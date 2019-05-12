package job2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;

public class Reducer2 extends Reducer<Text, Text, Text, Text> {
	
	private final int CLOSE = 0;
	private final int VOLUME = 1;
	private final int SECTOR = 3;
	
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		long volume = 0;
		double quotazione = 0;
		double variazione_percentuale = 0;
		String sector = null;
		List<Text> lista = new ArrayList<>();
		for(Text value : values) {
			lista.add(new Text(value.toString()));
		}
		List<Text> listaordinata = this.ordinaPerData(lista);
		
		double quotazione_inizio_anno = Double.parseDouble(listaordinata.get(0).toString().split(",")[1]);
		double quotazione_fine_anno = Double.parseDouble(listaordinata.get(listaordinata.size()-1).toString().split(",")[1]);

	
		for(Text value : lista) {
			String[] parts = value.toString().split(",");
			volume += Long.parseLong(parts[VOLUME]);
			quotazione += Double.parseDouble(parts[CLOSE]);
			sector = parts[SECTOR];
			
		}
		variazione_percentuale = ((quotazione_fine_anno - quotazione_inizio_anno)/quotazione_inizio_anno)*100;
		context.write(key, new Text(String.valueOf(volume) + "," + String.valueOf(quotazione) + "," + 
				String.format(Locale.ROOT, "%.2f", variazione_percentuale) + "," + sector ));
	}
	
	private ArrayList<Text> ordinaPerData(Iterable<Text> values) {

		ComparatorePerData cmp = new ComparatorePerData();

		ArrayList<Text> ordinatiPerData = new ArrayList<Text>();


		for (Text value : values) {
				ordinatiPerData.add(new Text(value.toString()));
		}

		Collections.sort(ordinatiPerData, cmp);
		return ordinatiPerData;
	}

}
