package calculateAverage;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DataUtil {
	
	public static FSDataInputStream openFileDFS(FileSystem fs, String pathname) throws IOException
	{
		Path path = new Path(pathname);
		FSDataInputStream fsd = fs.open(path);
		return fsd;
	}
	
	public static Long readline(FSDataInputStream fsd, StringBuffer buff, Long offset)
			throws IOException, UnsupportedEncodingException {
		
		boolean end = false;
		long begin = offset;
		while(!end)
		{
			byte[] lineBuff = new byte[4096];				
			int nbyte = fsd.read(begin, lineBuff, 0, lineBuff.length);				
			String line = new String(lineBuff, "UTF-8");
			
			int i;
			for(i = 0; i < line.length() && line.charAt(i) != '\n'; i++);
			
			if(i >= line.length())
				i = line.length() - 1;
			
			buff.append(offset + ": " + line.substring(0, i) + "\n");
			if(line.charAt(i) == '\n' || nbyte < 0)
			{
				buff.append("-----------------------\n");
				end = true;
			}
			else
			{
				offset += 4096;
			}
		}
		return offset;
	}
	
}
