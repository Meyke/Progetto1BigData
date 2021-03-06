package job2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Job2Mapper3 extends Mapper<LongWritable, Text, Text, Text> {
	
	private final int TICKER = 0;
    private final int CLOSE = 0;
    private final int VOLUME = 1;
    private final int DATE = 2;
    private final int SECTOR = 3;
    
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] split = line.split("\t");
		String[] parts = split[1].split(",");
	    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");  
	    Calendar calendar = new GregorianCalendar();
	    try { 
	    	Date date = formatter.parse(parts[DATE]); 
	    	calendar.setTime(date);
	    	int year = calendar.get(Calendar.YEAR);
	    	context.write(new Text(split[TICKER] + "," + year), new Text(parts[CLOSE] + "," 
	    			+ parts[VOLUME] + "," + parts[DATE] + "," + parts[SECTOR]));
	    	} catch(ParseException e) { 
	    		// TODO Auto-generated catch block 
	    		e.printStackTrace();
		  }
        
	}

}
