package calculateAverage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ResultReducer 
	extends Reducer<ResultKey,ResultRecord,Text,Text>{
	
	private Configuration conf;
	private String fileDir;
	
	@Override
	protected void setup(Reducer<ResultKey, ResultRecord, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		this.conf = context.getConfiguration();
		this.fileDir = this.conf.get("FileDir");
	}
	
	@Override
	protected void reduce(ResultKey key, Iterable<ResultRecord> values,
			Reducer<ResultKey, ResultRecord, Text, Text>.Context context) 
					throws IOException, InterruptedException {
		
		StringBuffer buff = new StringBuffer();
		buff.append(key.getFilename() + "\t" + key.getScore() + "\n");
		buff.append("***************************\n");
		
		FSDataInputStream fsd = DataUtil.openFileDFS(FileSystem.get(conf), 
				this.fileDir + "/" + key.getFilename());
		
		for(ResultRecord val : values)
		{
			for(RetrievalRecord r : val.getList())
			{
				for(Long offset : r.getOffsets())
				{
					StringBuffer line = new StringBuffer();
					DataUtil.readline(fsd, line, offset);
					buff.append(line.toString() + "\n");
				}
			}
		}
		context.write(new Text(""), new Text(buff.toString()));
	}

}
