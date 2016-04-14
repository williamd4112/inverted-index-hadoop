package calculateAverage;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper 
extends Mapper<LongWritable, Text, Text, InvertedIndexRecord> {

	static private Pattern termPattern = Pattern.compile("[A-Za-z]+");
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		// Get filename from inputsplit
		FileSplit split = (FileSplit) context.getInputSplit();
		String filename = split.getPath().getName();
		
		// Output pair [word; df; {fid,tf}]
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		while(tokenizer.hasMoreTokens())
		{
			String token = tokenizer.nextToken();
			Matcher matcher = termPattern.matcher(token);
			
			while(matcher.find())
			{
				String term = matcher.group();
				System.out.println(term);
				
				Integer fid = conf.getInt(filename, 0);
				assert(fid != null);
				
				InvertedIndexRecord record = new InvertedIndexRecord();
				assert(record != null);
				
				record.addFileRecord(new FileRecord(fid, key.get()));
				
				context.write(new Text(term), record);
			}
		}
	}

}
