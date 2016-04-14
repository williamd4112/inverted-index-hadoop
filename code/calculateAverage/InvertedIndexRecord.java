package calculateAverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.Writable;

public class InvertedIndexRecord implements Writable{
	
	private TreeSet<FileRecord> fileRecords = new TreeSet<FileRecord>();
		
	public void addFileRecord(FileRecord record)
	{
		// Merge with the other file records
		for(FileRecord r : this.fileRecords)
		{
			if(r.getFileID() == record.getFileID())
			{
				r.merge(record);
				
				return;
			}
		}
		
		// No same file id file record, add new one
		this.fileRecords.add(record);
	}

	public void merge(InvertedIndexRecord ir)
	{
		for(FileRecord r : ir.fileRecords)
			this.addFileRecord(r);
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		int df = input.readInt();
		this.fileRecords.clear();
		
		for(int i = 0; i < df; i++)
		{
			FileRecord frecord = new FileRecord();
			frecord.readFields(input);
			
			this.addFileRecord(frecord);
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(this.fileRecords.size());

		for(FileRecord r : this.fileRecords)
			r.write(output);
	}
	
	@Override
	public String toString()
	{
		StringBuffer buff = new StringBuffer();
		buff.append(String.valueOf(this.fileRecords.size()) + ";");
		
		for(FileRecord r : this.fileRecords)
		{
			buff.append(r.toString());
		}
		return buff.toString();
	}
}
