/**
 * Project Name: mrj-0.1
 * File Name: ImportTriplesReconstructReducerToCassandra.java
 * @author Gang Wu
 * 2014年10月28日 下午10:35:24
 * 
 * Description: 
 * Send reducer output to Cassandra DB by representing triples with ids
 */
package cn.edu.neu.mitt.mrj.importtriples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;

/**
 * @author gibeo_000
 *
 */
public class ImportTriplesReconstructReducerToCassandra extends
		Reducer<LongWritable, LongWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
	protected static Logger log = LoggerFactory.getLogger(ImportTriplesReconstructReducerToCassandra.class);

	private Triple oValue = new Triple();
//	private TripleSource oKey = new TripleSource();
	private Map<String, ByteBuffer> keys;
	private List<ByteBuffer> variables;
	
	
	// Init keys and variables
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer<LongWritable, LongWritable, Map<String, ByteBuffer>, List<ByteBuffer>>.Context context)
			throws IOException, InterruptedException {
		keys = new LinkedHashMap<String, ByteBuffer>();
		variables = new ArrayList<ByteBuffer>();
	}


	@Override
	protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		
		Iterator<LongWritable> itr = values.iterator();
		int counter = 0;
		while (itr.hasNext()) {
			counter++;
			
			//Get the position
			long value = itr.next().get();
			long pos = value & 0x3;
			if (pos == 0) {
				oValue.setSubject(value >> 2);
			} else if (pos == 1) {
				oValue.setPredicate(value >> 2);
			} else if (pos == 2) {
				oValue.setObject(value >> 2);
				oValue.setObjectLiteral(false);
			} else if (pos == 3) {
				oValue.setObject(value >> 2);
				oValue.setObjectLiteral(true);			
			} else {
				throw new IOException("Position not clear!!");
			}
		}
		
		if (counter != 3) {
			// Modified by WuGang 2010-12-3, 允许超过3元组出现，但是要报警！
			log.error("Found a non-triple when reconstructing. The count num is " + counter + ", and triple is " + oValue);
//			throw new IOException("Triple is not reconstructed!");
		}

		context.getCounter("output","records").increment(1);
		//context.write(oKey, oValue);
        
        
        // Prepare composite key (sub, pre, obj)
        keys.put(CassandraDB.COLUMN_SUB, ByteBufferUtil.bytes(oValue.getSubject()));
        keys.put(CassandraDB.COLUMN_PRE, ByteBufferUtil.bytes(oValue.getPredicate()));
        keys.put(CassandraDB.COLUMN_OBJ, ByteBufferUtil.bytes(oValue.getObject()));
        
        // Prepare variables 
        variables.add(ByteBufferUtil.bytes(oValue.getSubject()));
        variables.add(ByteBufferUtil.bytes(oValue.getPredicate()));
        variables.add(ByteBufferUtil.bytes(oValue.getObject()));
        context.write(keys, variables);
	}

	
	
}
