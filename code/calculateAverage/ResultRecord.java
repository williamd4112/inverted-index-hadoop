package calculateAverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class ResultRecord implements Writable{

	private ArrayList<RetrievalRecord> resultRecords =
			new ArrayList<RetrievalRecord>();
	
	public ResultRecord()
	{}
	
	public void addRecord(RetrievalRecord record)
	{
		this.resultRecords.add(record);
	}
	
	public ArrayList<RetrievalRecord> getList()
	{
		return this.resultRecords;
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		int count = input.readInt();
		this.resultRecords.clear();
		for(int i = 0; i < count; i++)
		{
			RetrievalRecord r = new RetrievalRecord();
			r.readFields(input);
			this.resultRecords.add(r);
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(this.resultRecords.size());
		for(int i = 0; i < this.resultRecords.size(); i++)
		{
			this.resultRecords.get(i).write(output);
		}
	}

}
