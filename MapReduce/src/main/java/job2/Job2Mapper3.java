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
	
    private final int OPEN = 0;
    private final int CLOSE = 1;
    private final int VOLUME = 2;
    private final int DATE = 3;
    private final int SECTOR = 4;
    
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
	    	context.write(new Text(parts[SECTOR] + "," + year), new Text(parts[OPEN] + "," +
	    				parts[CLOSE] + "," + parts[VOLUME] + "," + parts[DATE]));
	    	} catch(ParseException e) { 
	    		// TODO Auto-generated catch block 
	    		e.printStackTrace();
		  }
        
	}

}
