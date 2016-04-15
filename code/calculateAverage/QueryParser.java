package calculateAverage;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class QueryParser extends BooleanLogicParser {

	private HashSet<String> wordSet = new HashSet<String>();
	
	public QueryParser()  {
		super();
	}
	
	public void setWordSet(String...words)
	{
		wordSet.clear();
		wordSet.addAll(Arrays.asList(words));
	}
	
	public void setWordSet(List<String> words)
	{
		wordSet.clear();
		wordSet.addAll(words);
	}
	
	@Override
	protected Token evalID(String id) {
		
		return wordSet.contains(id) ? Token.TRUE : Token.FALSE;
	}

}
