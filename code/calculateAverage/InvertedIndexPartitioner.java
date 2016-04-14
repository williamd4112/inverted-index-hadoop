package calculateAverage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class InvertedIndexPartitioner 
	extends Partitioner<Text, InvertedIndexRecord> {
	
	@Override
	public int getPartition(Text key, InvertedIndexRecord value, int numReduceTasks) {
		int index = Character.toUpperCase((key.toString()).charAt(0)) - 'A';
		return index % numReduceTasks;
	}
}
