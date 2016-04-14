package calculateAverage;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RetrievalCombiner 
extends Reducer<RetrievalKey,RetrievalRecord,RetrievalKey,RetrievalRecord> {

	// Combiner implements method in Reducer
	public void reduce(RetrievalKey key, Iterable<RetrievalRecord> values, Context context) 
			throws IOException, InterruptedException {

		RetrievalRecord record = new RetrievalRecord();
		for(RetrievalRecord val : values)
		{
			System.out.println("Combine Key: " + key.getFilename() + "\tScore: " + val.getScore());
			record.or(val);
			context.write(
					key, record	);
		}

	}
}