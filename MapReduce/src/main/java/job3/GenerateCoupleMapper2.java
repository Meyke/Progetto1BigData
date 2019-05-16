package job3;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


//NOTA: di default la chiave del mapper è LongWritable e il value è Text.
// SE si vuole avere un altro formato, es. <Text,Text,...> , occorre estendere FileInputFormat
// dettagli qui: https://stackoverflow.com/questions/19624607/mapper-input-key-value-pair-in-hadoop

/**
 * Mapper per fare tante robe belle
 * @author micheletedesco1
 *
 */
public class GenerateCoupleMapper2 extends Mapper<LongWritable, Text, Text, StockTrend> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//WATT	-25,-18,-17,ENERGOUS CORPORATION,TECHNOLOGY
		String linea = value.toString();
        String[] items = linea.split("\\t");
		
        String ticker = items[0];
        String[] informations = items[1].split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        
        Integer trend2016 = Integer.parseInt(informations[0]);
        Integer trend2017 = Integer.parseInt(informations[1]);
        Integer trend2018 = Integer.parseInt(informations[2]);
        String companyName = informations[3];
        String sector = informations[4];
        
        Text trendTriennio = new Text(""+ new IntWritable(trend2016) + " " + new IntWritable(trend2017) + " " + new IntWritable(trend2018));
        StockTrend stocktrend = new StockTrend(new Text(ticker), 
        		                                   new IntWritable(trend2016), 
        		                                   new IntWritable(trend2017), 
        		                                   new IntWritable(trend2018), 
        		                                   new Text(companyName), 
        		                                   new Text(sector));               
		
		context.write(trendTriennio, stocktrend);
		
	}

}
