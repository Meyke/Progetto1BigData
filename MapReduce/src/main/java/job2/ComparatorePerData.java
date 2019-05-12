package job2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import org.apache.hadoop.io.Text;

public class ComparatorePerData implements Comparator<Text> {
	
	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	private Date dateo1;
	private Date dateo2;
    private final int DATE = 2;

	public int compare(Text o1, Text o2) {
	
			try {
				dateo1 = format.parse(o1.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")[DATE]);
				dateo2 = format.parse(o2.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")[DATE]);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
		return dateo1.compareTo(dateo2);
	}

}