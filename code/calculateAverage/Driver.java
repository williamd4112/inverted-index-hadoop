package calculateAverage;

public class Driver {
	
	public static void main(String[] args) throws Exception 
	{
		if(args[2].equals("r")) 
			Retrieval.retrieval(args);
		else if(args[2].equals("i")) 
			InvertedIndex.invertedIndex(args);
	}
}
