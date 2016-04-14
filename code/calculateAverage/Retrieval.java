package calculateAverage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Retrieval {
	
	public static void retrieval(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		conf.set("FileDir", args[3]);
		
		buildKeywordList(args, conf);
		
		int fileCounter = setupConfiguration(conf);
		conf.setInt("NumFiles", fileCounter);
		
		Job job = Job.getInstance(conf, "Retrieval");
		job.setJarByClass(Retrieval.class);
		
		// set the class of each stage in mapreduce
		job.setMapperClass(RetrievalMapper.class);
		job.setReducerClass(RetrievalReducer.class);
		
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(RetrievalRecord.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	private static int setupConfiguration(Configuration conf) throws IOException, UnsupportedEncodingException {
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

	private static void buildKeywordList(String[] args, Configuration conf) {
		// Set first keyword set
		conf.setStrings("Keywords", args[4].split("\\s+"));
		
		System.out.println(args[4]);
		for(String s : conf.getStrings("Keywords"))
		{
			System.out.println("Keyword\t" + s);
		}
	}
}
