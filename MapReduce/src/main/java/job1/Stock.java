package job1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class Stock implements WritableComparable {

	private Text ticker;
	private FloatWritable close;
	private FloatWritable low;
	private FloatWritable high;
	private IntWritable volume;
	private Text data;
	
	

	// hadoop richiede che ci sia un costruttore senza argomenti e inizializzato
	public Stock() {
		this.ticker = new Text();
		this.close = new FloatWritable();
		this.low = new FloatWritable();
		this.high = new FloatWritable();
		this.volume = new IntWritable();
		this.data = new Text();
		
	}

	public Stock(Text ticker, FloatWritable close, FloatWritable low, FloatWritable high, IntWritable volume,
			Text data) {
		this.ticker = new Text(ticker.toString());
		this.close = new FloatWritable(close.get());
		this.low = new FloatWritable(low.get());
		this.high = new FloatWritable(high.get());
		this.volume = new IntWritable(volume.get());
		this.data = new Text(data.toString());
	}

	public Text getTicker() {
		return ticker;
	}

	public void setTicker(Text ticker) {
		this.ticker = ticker;
	}

	public FloatWritable getClose() {
		return close;
	}


	public void setClose(FloatWritable close) {
		this.close = close;
	}

	public FloatWritable getLow() {
		return low;
	}

	public void setLow(FloatWritable low) {
		this.low = low;
	}

	public FloatWritable getHigh() {
		return high;
	}

	public void setHigh(FloatWritable high) {
		this.high = high;
	}

	public IntWritable getVolume() {
		return volume;
	}

	public void setVolume(IntWritable volume) {
		this.volume = volume;
	}

	public Text getData() {
		return data;
	}

	public void setData(Text data) {
		this.data = data;
	}
	

	public void readFields(DataInput in) throws IOException {
		ticker.readFields(in);
		low.readFields(in);
		close.readFields(in);
		high.readFields(in);
		volume.readFields(in);
		data.readFields(in);	
	}

	
	public void write(DataOutput out) throws IOException {
		ticker.write(out);
		low.write(out);
		close.write(out);
		high.write(out);
		volume.write(out);
		data.write(out);	
	}

	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String toString() {
		return "Stock [ticker=" + ticker + ", close=" + close + ", low=" + low + ", high=" + high + ", volume=" + volume
				+ ", data=" + data + "]";
	}
	
	

}
