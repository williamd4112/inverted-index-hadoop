package calculateAverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ResultKey implements WritableComparable<ResultKey> {

	private String filename;
	private double score;
	
	public ResultKey(){}
	public ResultKey(String filename, double score)
	{
		this.filename = filename;
		this.score = score;
	}
	
	public String getFilename()
	{
		return this.filename;
	}
	
	public double getScore()
	{
		return this.score;
	}	
	
	@Override
	public void readFields(DataInput input) throws IOException {
		this.filename = input.readUTF();
		this.score = input.readDouble();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(filename);
		output.writeDouble(score);
	}

	@Override
	public int compareTo(ResultKey k) {

		int result = Double.compare(score, k.getScore());
		if(result == 0)
			result = filename.compareTo(k.filename);
		return result;
	}
	
	@Override
	public String toString()
	{
		return filename + ":" + score; 
	}

}
