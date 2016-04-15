package calculateAverage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

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
	private QueryParser parser;
	
	@Override
	protected void setup(Reducer<Text, RetrievalRecord, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		this.conf = context.getConfiguration();
		this.parser = new QueryParser();
	}

	public void reduce(Text key, Iterable<RetrievalRecord> values, Context context) 
			throws IOException, InterruptedException {		
		
		StringBuffer buff = new StringBuffer();
		ArrayList<String> terms = new ArrayList<String>();
		for(RetrievalRecord value : values)
		{
			terms.add(value.getTerm());
			buff.append(value.toString() + ";");
		}
	
		try {
			this.parser.setWordSet(terms);
			this.parser.parse(conf.get("Query"));
			if(this.parser.eval())
				context.write(key, new Text(buff.toString()));
		} catch (BooleanLogicParserException e) {
			e.printStackTrace();
		}
	}


}