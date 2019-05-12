package job2;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.text.ParseException;
import java.text.SimpleDateFormat;  

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper1 extends Mapper<LongWritable, Text, Text, Text>{
	
	private final int TICKER = 0;
    private final int CLOSE = 2;
    private final int VOLUME = 6;
    private final int DATE = 7;
    
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] parts = line.split(",");
	    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");  
	    Calendar calendar = new GregorianCalendar();
	    try { 
	    	Date date = formatter.parse(parts[DATE]); 
	    	calendar.setTime(date);
	    	if(calendar.get(Calendar.YEAR) >= 2004) {
	    		context.write(new Text(parts[TICKER]), new Text(parts[CLOSE] + "," 
	    				+ parts[VOLUME] + "," + parts[DATE])); } 
	    	} catch(ParseException e) { 
	    		// TODO Auto-generated catch block 
	    		e.printStackTrace();
		  }
	}
}
