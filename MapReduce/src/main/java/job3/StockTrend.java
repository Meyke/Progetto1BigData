package job3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StockTrend implements WritableComparable{

	private Text ticker;
	private IntWritable trend2016;
	private IntWritable trend2017;
	private IntWritable trend2018;
	private Text companyName; // a quale società appartiene questa azione
	private Text sector;
	
	public StockTrend() {
		this.ticker = new Text();
		this.trend2016 = new IntWritable();
		this.trend2017 = new IntWritable();
		this.trend2018 = new IntWritable();
		this.companyName = new Text();
		this.sector = new Text();
	}
	
	

	public StockTrend(Text ticker, IntWritable trend2016, IntWritable trend2017, IntWritable trend2018, Text companyName, Text sector) {
		this.ticker = new Text(ticker.toString());
		this.trend2016 = new IntWritable(trend2016.get());
		this.trend2017 = new IntWritable(trend2017.get());
		this.trend2018 = new IntWritable(trend2018.get());
		this.companyName = new Text(companyName.toString());
		this.sector = new Text(sector.toString());
	}
	
	



	public Text getTicker() {
		return ticker;
	}



	public void setTicker(Text ticker) {
		this.ticker = ticker;
	}
	

	public Text getCompanyName() {
		return companyName;
	}



	public void setCompanyName(Text companyName) {
		this.companyName = companyName;
	}



	public IntWritable getTrend2016() {
		return trend2016;
	}



	public void setTrend2016(IntWritable trend2016) {
		this.trend2016 = trend2016;
	}



	public IntWritable getTrend2017() {
		return trend2017;
	}



	public void setTrend2017(IntWritable trend2017) {
		this.trend2017 = trend2017;
	}



	public IntWritable getTrend2018() {
		return trend2018;
	}



	public void setTrend2018(IntWritable trend2018) {
		this.trend2018 = trend2018;
	}
	
	

	public Text getSector() {
		return sector;
	}



	public void setSector(Text sector) {
		this.sector = sector;
	}



	public void readFields(DataInput in) throws IOException {
		ticker.readFields(in);
		trend2016.readFields(in);
		trend2017.readFields(in);
		trend2018.readFields(in);
		companyName.readFields(in);
		sector.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		ticker.write(out);
		trend2016.write(out);
		trend2017.write(out);
		trend2018.write(out);
		companyName.write(out);
		sector.write(out);
	}

	public int compareTo(Object o) {
		return 0;
	}



	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((trend2016 == null) ? 0 : trend2016.hashCode());
		result = prime * result + ((trend2017 == null) ? 0 : trend2017.hashCode());
		result = prime * result + ((trend2018 == null) ? 0 : trend2018.hashCode());
		return result;
	}



	@Override
	public boolean equals(Object obj) {
		StockTrend that = (StockTrend) obj;
		return this.trend2016.equals(that.getTrend2016()) &&
				this.trend2017.equals(that.getTrend2017()) &&
				this.trend2018.equals(that.getTrend2018());
				
	}



	@Override
	public String toString() {
		return "StockTrend [ticker=" + ticker + ", trend2016=" + trend2016 + ", trend2017=" + trend2017 + ", trend2018="
				+ trend2018 + ", companyName=" + companyName + ", sector=" + sector + "]";
	}



	
	
	
	
	

}
