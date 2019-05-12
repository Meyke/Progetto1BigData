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

public class Job2Mapper4 extends Mapper<LongWritable, Text, Text, Text> {
	
    private final int VOLUME = 0;
    private final int CLOSE = 1;
    private final int VARIAZIONE = 2;
    private final int SECTOR = 3;
    
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] split = line.split("\t");
		String[] parts = split[1].split(",");
		int year = Integer.parseInt(split[0].split(",")[1]);
	    context.write(new Text(parts[SECTOR] + "," + year), new Text(parts[VOLUME] + "," +
    				parts[CLOSE] + "," + parts[VARIAZIONE]));
	    }

}
