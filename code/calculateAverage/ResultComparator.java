package calculateAverage;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ResultComparator extends WritableComparator { 
	
	public ResultComparator() {
		super(ResultKey.class, true);
	}
	
	
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable o1, WritableComparable o2) {
		ResultKey k1 = (ResultKey)o1;
		ResultKey  k2 = (ResultKey)o2;
		
		return -k1.compareTo(k2);
	}
}
