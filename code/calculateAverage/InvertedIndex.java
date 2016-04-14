package calculateAverage;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class InvertedIndex {
	
	public static void invertedIndex(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		
		int fileCounter = 0;
		
		Path path = new Path(args[0]);
		FileStatus[] status = FileSystem.get(conf).listStatus(path);
		FSDataOutputStream fsStream = FileSystem.get(conf).create(new Path("./filetable"));
		BufferedWriter out =
	              new BufferedWriter(new OutputStreamWriter(fsStream,"UTF-8"));
		for(FileStatus s : status)
		{
			out.write(String.format("%d\t%s\n",fileCounter,s.getPath().getName()));
			conf.setInt(s.getPath().getName(), fileCounter);
			fileCounter++;
		}
		out.flush();
		out.close();
		
		Job job = Job.getInstance(conf, "InvertedIndex");
		job.setJarByClass(InvertedIndex.class);
		
		// set the class of each stage in mapreduce
		job.setMapperClass(InvertedIndexMapper.class);
		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setPartitionerClass(InvertedIndexPartitioner.class);
		job.setSortComparatorClass(InvertedIndexComparator.class);
		job.setReducerClass(InvertedIndexReducer.class);
		
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InvertedIndexRecord.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// set the number of reducer
		job.setNumReduceTasks(26);
		
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	

}
