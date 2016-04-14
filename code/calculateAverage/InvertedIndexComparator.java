package calculateAverage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class InvertedIndexComparator 
	extends WritableComparator {
	
	public InvertedIndexComparator () {
		super(Text.class, true);
	}
	
	
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable o1, WritableComparable o2) {
        Text key1 = (Text) o1;
        Text key2 = (Text) o2;

        // TODO Order by A -> a -> B -> b .... 
        String s1 = key1.toString();
        String s2 = key2.toString();

        int i;
        for(i = 0; i < s1.length() && i < s2.length(); i++)
        {
                int c1 = (Character.isLowerCase(s1.charAt(i))) ?
                                (Character.toUpperCase(s1.charAt(i)) - 'A') * 2 :
                                (s1.charAt(i) - 'A') * 2 - 1;
                int c2 = (Character.isLowerCase(s2.charAt(i))) ?
                                (Character.toUpperCase(s2.charAt(i)) - 'A') * 2 :
                                (s2.charAt(i) - 'A') * 2 - 1;
                if(c1 < c2) return -1;
                else if(c1 > c2) return 1;
                else continue;
        }

        if(s1.length() < s2.length())
                return -1;
        else if(s1.length() > s2.length())
                return 1;
        else
                return 0;
	}
	
}