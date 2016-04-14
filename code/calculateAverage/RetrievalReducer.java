package calculateAverage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.LineReader;

public class RetrievalReducer 
	extends Reducer<Text,RetrievalRecord,Text,Text> {
	
	private Configuration conf;
	
	@Override
	protected void setup(Reducer<Text, RetrievalRecord, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		this.conf = context.getConfiguration();
	}

	public void reduce(Text key, Iterable<RetrievalRecord> values, Context context) 
			throws IOException, InterruptedException {
		StringBuffer buff = new StringBuffer();
		for(RetrievalRecord value : values)
		{
			buff.append(value.toString() + ";");
		}
		context.write(key, new Text(buff.toString()));
	}


}