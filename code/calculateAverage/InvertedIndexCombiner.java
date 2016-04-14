package calculateAverage;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexCombiner 
	extends Reducer<Text,InvertedIndexRecord,Text,InvertedIndexRecord> {
	
	// Combiner implements method in Reducer
    public void reduce(Text key, Iterable<InvertedIndexRecord> values, Context context) 
    		throws IOException, InterruptedException {
		
    	InvertedIndexRecord record = new InvertedIndexRecord();
    	for(InvertedIndexRecord r : values)
    	{
    		record.merge(r);
    	}
    	context.write(key, record);
	}
}
