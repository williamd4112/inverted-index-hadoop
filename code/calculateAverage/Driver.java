package calculateAverage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Driver {
	
	public static void main(String[] args) throws Exception 
	{
		if(args[2].equals("r")) 
			Retrieval.retrieval(args);
		else if(args[2].equals("i")) 
			InvertedIndex.invertedIndex(args);
		else if(args[2].equals("rs"))
			Result.result(args);
	}
	
	public static int loadFileTable(Configuration conf) throws IOException
	{
		int fileCounter = 0;
		Path path = new Path("./filetable");
		FSDataInputStream fsd = new FSDataInputStream(
				FileSystem.get(conf).open(path));
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(fsd, "UTF-8"));
	
		String line;
		while((line = reader.readLine()) != null)
		{
			String[] id_name = line.split("\\s+");
			conf.set("file" + id_name[0], id_name[1]);
			System.out.println("file" + id_name[0] + "\t" + id_name[1]);
			fileCounter++;
		}
		return fileCounter;
	}
}
