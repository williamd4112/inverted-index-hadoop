package calculateAverage;

import java.util.HashSet;

public class Query {
	
	private HashSet<String> keywordSet = new HashSet<String>();
	
	public Query(String[] keywords)
	{
		for(String keyword : keywords)
			this.keywordSet.add(keyword);
	}
	
	public boolean match(String word)
	{
		return this.keywordSet.contains(word);
	}
}
