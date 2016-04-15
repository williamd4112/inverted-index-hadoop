package calculateAverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class RetrievalRecord implements Writable{

	private String term;
	private int df;
	private ArrayList<Long> offsets = new ArrayList<Long>();
	
	public RetrievalRecord()
	{}
	
	public RetrievalRecord(String term, int df)
	{
		this.term = term;
		this.df = df;
	}
	
	public void addOffset(long offset)
	{
		this.offsets.add(offset);
	}
	
	public String getTerm()
	{
		return this.term;
	}
	
	public void or(RetrievalRecord r)
	{
		for(Long offset : r.offsets)
		{
			this.addOffset(offset);
		}
	}
	
	public double calculateScore(int N)
	{
		return this.offsets.size() * Math.log((double)N / (double)df);
	}
	
	public ArrayList<Long> getOffsets()
	{
		return this.offsets;
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		this.term = input.readUTF();
		this.df = input.readInt();
		this.offsets.clear();
		int n = input.readInt();
		for(int i = 0; i < n; i++)
			this.offsets.add(input.readLong());
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(this.term);
		output.writeInt(this.df);
		output.writeInt(this.offsets.size());
		for(Long offset : this.offsets)
			output.writeLong(offset);
	}
	
	@Override
	public String toString()
	{
		boolean first = true;
		StringBuffer buff = new StringBuffer();
		buff.append(this.term + " " + this.df + " ");
		buff.append("[");
		for(Long offset : offsets)
		{
			if(!first) buff.append(",");
			buff.append(offset);
			first = false;
		}
		buff.append("]");
		return buff.toString();
	}

}
