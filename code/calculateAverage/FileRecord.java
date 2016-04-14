package calculateAverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class FileRecord implements Writable, Comparable<FileRecord>
{
	private int fid;
	private ArrayList<Long> offsets = new ArrayList<Long>();
	
	public FileRecord()
	{
		this(0, -1);
	}
	
	public FileRecord(int fid, long offset)
	{
		this.fid = fid;
		this.offsets.add(offset);
	}
	
	public int getFileID()
	{
		return fid;
	}
	
	public long getFrequency()
	{
		return this.offsets.size();
	}
	
	public void merge(FileRecord fr)
	{
		for(Long offset : fr.offsets)
			this.offsets.add(offset);
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		fid = input.readInt();
		
		long tf = input.readLong();
		
		this.offsets.clear();
		for(int i = 0; i < tf; i++)
		{
			this.offsets.add(input.readLong());
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(fid);
		output.writeLong(this.offsets.size());
		
		for(Long offset : offsets)
			output.writeLong(offset.longValue());
	}

	@Override
	public int compareTo(FileRecord f) {
		if(fid < f.fid) return -1;
		else if (fid > f.fid) return 1;
		else return 0;
	}
	
	@Override
	public String toString()
	{
		StringBuffer buff = new StringBuffer();
		buff.append(String.format("%d %d ",this.fid, this.getFrequency()));
		buff.append("[");
		boolean first = true;
		for(Long offset : this.offsets)
		{
			if(!first) buff.append(",");
			buff.append(offset);
			first = false;
		}
		buff.append("];");
		
		return buff.toString();
	}
	
}