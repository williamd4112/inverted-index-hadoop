package calculateAverage;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RetrievalComparator 
	extends WritableComparator {
	
	public RetrievalComparator () {
		super(RetrievalKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {
		RetrievalKey r1 = (RetrievalKey)o1;
		RetrievalKey r2 = (RetrievalKey)o2;
		
		return r1.compareTo(r2);
	}	
}
