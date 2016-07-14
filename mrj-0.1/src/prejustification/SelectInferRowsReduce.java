package prejustification;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
public class SelectInferRowsReduce extends Reducer<Map<String, ByteBuffer>, ByteBuffer,  Map<String, ByteBuffer>, ByteBuffer> {
	public void reduce(Map<String, ByteBuffer> key, Iterable<ByteBuffer> value, Context context) throws IOException, InterruptedException{

		for (ByteBuffer inferredsteps : value) {
			System.out.println(key);
			context.write(key, inferredsteps);
		}
		
	}

}
