package job1;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;

public class ComparatorePerData implements Comparator<Stock> {
	
	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	private Date dateo1;
	private Date dateo2;
	
	public int compare(Stock o1, Stock o2) {
	
			try {
				dateo1 = format.parse(o1.getData().toString().trim());
				dateo2 = format.parse(o2.getData().toString().trim());
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
		return dateo1.compareTo(dateo2);
	}

}
