package calculateAverage;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RetrievalMapper 
	extends Mapper<LongWritable, Text, Text, RetrievalRecord> {
	
	private Configuration conf;
	private Query query;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, RetrievalRecord>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		this.conf = context.getConfiguration();
		this.query = new Query(this.conf.getStrings("Keywords"));
	}

	/**
	 * map
	 * output:
	 * 	<filename, {term df [offset1, offset2, ...]}>
	 */
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String[] parts = value.toString().split(";");	
		String[] term_df = parts[0].split("\\s+");
		
		String term = term_df[0];
		
		if(query.match(term))
		{
			int df = Integer.valueOf(term_df[1]);
			
			for(int i = 1; i < parts.length; i++)
			{
				String[] fid_tf = parts[i].split("\\s+");
				String filename = findFilename(Integer.valueOf(fid_tf[0]));
				long tf = Long.valueOf(fid_tf[1]);
				
				RetrievalRecord value_record =
						new RetrievalRecord(term, df);
				
				// Parse offset
				String offsets_str = fid_tf[2].substring(1, fid_tf[2].length() - 1);
				String[] offsets = offsets_str.split(",");

				for(String offset : offsets)
				{
					value_record.addOffset(Long.valueOf(offset));
				}
				context.write(new Text(filename), value_record);
			}
		}
	}

	private String findFilename(int fid) {
		String fid_str = String.format("file%d",fid);
		String filename = conf.get(fid_str);
		return filename;
	}
}
