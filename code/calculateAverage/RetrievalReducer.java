package calculateAverage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.LineReader;

public class RetrievalReducer 
extends Reducer<RetrievalKey,RetrievalRecord,Text,Text> {
	
	public void reduce(RetrievalKey key, Iterable<RetrievalRecord> values, Context context) 
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		RetrievalRecord record = new RetrievalRecord();
		for(RetrievalRecord r : values)
		{
			record.or(r);
		}
	
		System.out.println(key.getFilename().toString() + " : " + record.getScore());
		
		Path path = new Path(conf.get("FileDir") + "/" + key.getFilename());
		FSDataInputStream fsd = FileSystem.get(conf).open(path);
		
		StringBuffer buff = new StringBuffer();
		buff.append("************************\n");
		for(Long offset : record.getOffsets())
		{
			boolean end = false;
			long begin = offset;
			while(!end)
			{
				byte[] lineBuff = new byte[4096];				
				int nbyte = fsd.read(begin, lineBuff, 0, lineBuff.length);				
				String line = new String(lineBuff, "UTF-8");
				
				int i;
				for(i = 0; i < line.length() && line.charAt(i) != '\n'; i++);
				
				if(i >= line.length())
					i = line.length() - 1;
				
				buff.append(offset + ": " + line.substring(0, i) + "\n");
				if(line.charAt(i) == '\n' || nbyte < 0)
				{
					buff.append("-----------------------\n");
					end = true;
				}
				else
				{
					offset += 4096;
				}
			}
		}
					
		context.write(
				new Text(
						String.format("Rank: %d\t%s\t", 
								conf.getInt("Rank", 0), key.getFilename())), 
				new Text(record.toString() + "\n" + buff.toString()));
		
		conf.setInt("Rank", conf.getInt("Rank", 0) + 1);
	}
}