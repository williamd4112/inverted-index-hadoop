package calculateAverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class RetrievalKey 
	implements WritableComparable<RetrievalKey>{

	private int fid;
	private String filename;
	private double score;
	
	public RetrievalKey(int fid, String filename, double score)
	{
		this.fid = fid;
		this.filename = filename;
		this.score = score;
	}
	
	public RetrievalKey()
	{
		this.fid = 0;
		this.filename = "";
		this.score = 0;
	}
	
	public String getFilename()
	{
		return this.filename;
	}
	
	public double getScore()
	{
		return this.score;
	}
	
	public int getFileID()
	{
		return this.fid;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		this.fid = input.readInt();
		this.filename = input.readUTF();
		this.score = input.readDouble();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(this.fid);
		output.writeUTF(this.filename);
		output.writeDouble(this.score);
	}

	@Override
	public int compareTo(RetrievalKey other) {
		if(this.score < other.getScore()) return 1;
		else if(this.score > other.getScore()) return -1;
		else {
			return this.filename.compareTo(other.filename);
		}
	}

}
