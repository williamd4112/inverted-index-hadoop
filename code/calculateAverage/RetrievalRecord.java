package calculateAverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class RetrievalRecord implements Writable{

	private double score;
	private ArrayList<Long> offsets = new ArrayList<Long>();
	
	public RetrievalRecord(double score)
	{
		this.score = score;
	}
	
	public RetrievalRecord()
	{
		this.score = 0;
	}
	
	public double getScore()
	{
		return this.score;
	}
	
	public void addOffset(long offset)
	{
		this.offsets.add(offset);
	}
	
	public void or(RetrievalRecord r)
	{
		for(Long offset : r.offsets)
		{
			this.addOffset(offset);
		}
		this.score += r.score;
	}
	
	public ArrayList<Long> getOffsets()
	{
		return this.offsets;
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		this.score = input.readDouble();
		this.offsets.clear();
		int n = input.readInt();
		for(int i = 0; i < n; i++)
			this.offsets.add(input.readLong());
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeDouble(this.score);
		output.writeInt(this.offsets.size());
		for(Long offset : this.offsets)
			output.writeLong(offset);
	}
	
	@Override
	public String toString()
	{
		return String.format("score =  %f",score);
	}

}
