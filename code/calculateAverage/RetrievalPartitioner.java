package calculateAverage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RetrievalPartitioner
extends Partitioner<RetrievalKey, RetrievalRecord>{

	@Override
	public int getPartition(RetrievalKey key, RetrievalRecord value, int numReduceTasks) {
		System.out.println("Key " + 
	key.getFilename() + "(" + key.getFileID() + ")" + " to " + (key.getFileID() % numReduceTasks));
		return key.getFileID() % numReduceTasks;
	}

}
