package calculateAverage;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RetrievalMapper 
	extends Mapper<LongWritable, Text, RetrievalKey, RetrievalRecord> {
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		String[] parts = value.toString().split(";");	
		String[] term_df = parts[0].split("\\s+");
		
		String term = term_df[0];
		if(!conf.getStringCollection("Keywords").contains(term))
			return;
		
		long df = Long.valueOf(term_df[1]);
		
		for(int i = 1; i < parts.length; i++)
		{
			String[] fid_tf = parts[i].split("\\s+");
			int fid = Integer.valueOf(fid_tf[0]);
			long tf = Long.valueOf(fid_tf[1]);
			
			String fid_str = String.format("file%d",fid);
			String filename = conf.get(fid_str);
			
			double w = tf * Math.log((double)conf.getInt("NumFiles", 0) / (double)df);
			
			RetrievalRecord value_record = new RetrievalRecord(w);
			String offsets_str = fid_tf[2].substring(1, fid_tf[2].length() - 1);
			String[] offsets = offsets_str.split(",");

			for(String offset : offsets)
			{
				value_record.addOffset(Long.valueOf(offset));
			}
			
			RetrievalKey rkey = new RetrievalKey(fid, filename, w);
			context.write(
					rkey, 
					value_record);
			System.out.println("MapKey: " + filename + "\tScore: " + rkey.getScore());
		}
	}
}
