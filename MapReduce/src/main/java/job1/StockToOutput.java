package job1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StockToOutput implements WritableComparable {
	
	private Text ticker;
	private FloatWritable incrementoPercentuale;
	private FloatWritable minlow;
	private FloatWritable maxhigh;
	private FloatWritable meanvolumedaily;
	
	// hadoop richiede che ci sia un costruttore senza argomenti e inizializzato
	public StockToOutput() {
		this.ticker = new Text();
		this.incrementoPercentuale = new FloatWritable();
		this.minlow = new FloatWritable();
		this.maxhigh = new FloatWritable();
		this.meanvolumedaily = new FloatWritable();
	}
	
	public StockToOutput(Text ticker, FloatWritable incrementoPercentuale, FloatWritable minlow, FloatWritable maxhigh,
			FloatWritable meanvolumedaily) {
		this.ticker = ticker;
		this.incrementoPercentuale = incrementoPercentuale;
		this.minlow = minlow;
		this.maxhigh = maxhigh;
		this.meanvolumedaily = meanvolumedaily;
	}

	public Text getTicker() {
		return ticker;
	}

	public void setTicker(Text ticker) {
		this.ticker = ticker;
	}

	public FloatWritable getIncrementoPercentuale() {
		return incrementoPercentuale;
	}

	public void setIncrementoPercentuale(FloatWritable incrementoPercentuale) {
		this.incrementoPercentuale = incrementoPercentuale;
	}

	public FloatWritable getMinlow() {
		return minlow;
	}

	public void setMinlow(FloatWritable minlow) {
		this.minlow = minlow;
	}

	public FloatWritable getMaxhigh() {
		return maxhigh;
	}

	public void setMaxhigh(FloatWritable maxhigh) {
		this.maxhigh = maxhigh;
	}

	public FloatWritable getMeanvolumedaily() {
		return meanvolumedaily;
	}

	public void setMeanvolumedaily(FloatWritable meanvolumedaily) {
		this.meanvolumedaily = meanvolumedaily;
	}

	public void readFields(DataInput in) throws IOException {
		ticker.readFields(in);
		incrementoPercentuale.readFields(in);
		minlow.readFields(in);
		maxhigh.readFields(in);
		meanvolumedaily.readFields(in);
		
	}

	public void write(DataOutput out) throws IOException {
		ticker.write(out);
		incrementoPercentuale.write(out);
		minlow.write(out);
		maxhigh.write(out);
		meanvolumedaily.write(out);
		
	}

	public int compareTo(Object o) {
		return 0;
	}

	@Override
	public String toString() {
		return " [ticker=" + ticker + ", incrementoPercentuale=" + incrementoPercentuale + ", minlow="
				+ minlow + ", maxhigh=" + maxhigh + ", meanvolumedaily=" + meanvolumedaily + "]";
	}
	
	
	
	

}
