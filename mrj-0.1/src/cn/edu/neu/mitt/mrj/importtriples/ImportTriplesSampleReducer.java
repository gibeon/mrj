package cn.edu.neu.mitt.mrj.importtriples;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ImportTriplesSampleReducer extends Reducer<Text, NullWritable, LongWritable, BytesWritable> {

	private long threshold = 0;
	private long counter = 0;
	
	private LongWritable oKey = new LongWritable();
	private BytesWritable oValue = new BytesWritable();

	public void reduce(Text key, Iterable<NullWritable> values,  Context context) throws IOException, InterruptedException { 
		long count = 0;
		Iterator<NullWritable> itr = values.iterator();
		while (itr.hasNext()) {
			count++;			
			itr.next();
		}

		if (count > threshold) {
			oKey.set(counter++);
			byte[] bytes = key.toString().getBytes();
			oValue.set(bytes, 0, bytes.length);
			context.write(oKey, oValue);
			context.getCounter("output", "records").increment(1);
		}
	}

    protected void setup(Context context) throws IOException, InterruptedException {
		threshold = context.getConfiguration().getInt("reasoner.threshold", 0);
		//Set counter
		String taskId = context.getConfiguration().get("mapred.task.id").substring(
				context.getConfiguration().get("mapred.task.id").indexOf("_r_") + 3);
		taskId = taskId.replaceAll("_", "");
		counter = (Long.valueOf(taskId)) << 13;
		if (counter == 0) { counter +=100; }
    }
}
