package calculateAverage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Result {

	/**
	 * <input(result of retrieval)> <output(result of sorted retrieval)> <rs> <input_files>
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void result(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		conf.set("FileDir", args[3]);
		
		int numFiles = Driver.loadFileTable(conf);
		conf.setInt("NumFiles", numFiles);
		
		Job job = Job.getInstance(conf, "Result");
		job.setJarByClass(Result.class);
		
		// set the class of each stage in mapreduce
		job.setMapperClass(ResultMapper.class);
		job.setSortComparatorClass(ResultComparator.class);
		job.setReducerClass(ResultReducer.class);
		
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(ResultKey.class);
		job.setMapOutputValueClass(ResultRecord.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
