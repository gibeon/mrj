package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;

import com.datastax.driver.core.Row;

public class OWLSameAsMapper extends Mapper<Long, Row, LongWritable, BytesWritable> {

	private LongWritable oKey = new LongWritable();
	private BytesWritable oValue = new BytesWritable();
	private byte[] bValue = new byte[9];
	
	/* Flags:
	 * 0 : the pair is of the "k" type
	 * 1 : the pair is of the "group" type */
	public void map(Long key, Row row, Context context) throws IOException, InterruptedException {
//		System.out.println("In OWLSameAsMapper.");
		Triple value = CassandraDB.readJustificationFromMapReduceRow(row);

		
		/* Source triple: s owl:sameAs o */
		long olKey = 0;
		long olValue = 0;
		if (value.getSubject() > value.getObject()) {	//key里面是大值，value里是小值
			olKey = value.getSubject();
			olValue = value.getObject();
		} else {
			olKey = value.getObject();
			olValue = value.getSubject();
		}
		
		// 用最小那个值标识每一个组
		
		oKey.set(olKey);
		bValue[0] = 0;
		NumberUtils.encodeLong(bValue, 1, olValue);		
		oValue.set(bValue, 0, bValue.length);
		context.write(oKey, oValue);	//最终key是大值，value是小值，用这个可以得知每一个resource，属于哪个组
		
		oKey.set(olValue);
		bValue[0] = 1;
		NumberUtils.encodeLong(bValue, 1, olKey);		
		oValue.set(bValue, 0, bValue.length);
		context.write(oKey, oValue);	//最终key是小值，value是大值，用这个可以得知每一个组中包含哪些resource
	}
}
