package calculateAverage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ResultMapper 
	extends Mapper<LongWritable, Text, ResultKey, ResultRecord>{
	
	private Configuration conf;
	private int numFiles;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, ResultKey, ResultRecord>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		this.conf = context.getConfiguration();
		this.numFiles = this.conf.getInt("NumFiles", 0);
	}
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, ResultKey, ResultRecord>.Context context)
			throws IOException, InterruptedException {
		
		String[] parts = value.toString().split("\\s+", 2);
		
		String filename = parts[0];
		String[] record_strs = parts[1].split(";");
		
		System.out.println(filename);
		
		ResultRecord resultRecord = new ResultRecord();
		
		long tfSum = 0;
		long dfSum = 0;
		
		// Iterate RetrievalRecord and calculate score
		for(String record_str : record_strs)
		{
			String[] field_strs = record_str.split("\\s+");
			String term = field_strs[0];
			int df = Integer.valueOf(field_strs[1]);
			dfSum += df;
			
			System.out.println(term + " : " + df);
			
			RetrievalRecord record = new RetrievalRecord(term, df);
			
			String[] offset_strs = field_strs[2].
					substring(1, field_strs[2].length() - 1).split(",");
			for(String offset_str : offset_strs){
				System.out.println(offset_str);
				record.addOffset(Long.valueOf(offset_str));
				tfSum++;
			}
			resultRecord.addRecord(record);
		}
	
		double score = tfSum * Math.log((double)numFiles / (double)dfSum);
		
		System.out.println("Total Score: " + score);
		context.write(new ResultKey(filename, score), resultRecord);
	}

}
