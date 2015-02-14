/**
 * 
 */
package cn.edu.neu.mitt.mrj.justification;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.io.dbs.SimpleClientDataStax;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;

/**
 * @author gibeon
 *
 */
public class OWLHorstJustificationReducer extends
		Reducer<MapWritable, LongWritable, Triple, MapWritable> {

//	private static Logger log = LoggerFactory.getLogger(OWLHorstJustificationReducer.class);
	private static SimpleClientDataStax sClient = null;

	@Override
	protected void reduce(MapWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		
		long total = 0; 
		for (LongWritable count:values){
			total += count.get();
		}
//		System.out.println("Total count is: " + total);
		
		if (total == key.size()){	// Find a candidate justification, output it to the database
			Set<TupleValue> resultJustification = new HashSet<TupleValue>();
			for(Writable triple : key.keySet()){
				TupleType theType = TupleType.of(DataType.bigint(), DataType.bigint(), DataType.bigint());
				TupleValue theValue = theType.newValue();
				theValue.setLong(0, ((Triple)triple).getSubject());
				theValue.setLong(1, ((Triple)triple).getPredicate());
				theValue.setLong(2, ((Triple)triple).getObject());
				resultJustification.add(theValue);
			}
			
//			log.info("Write a candidate justification to database=========== ");
//			log.info(resultJustification.toString());
			
			Insert insert = QueryBuilder
					.insertInto(CassandraDB.KEYSPACE, CassandraDB.COLUMNFAMILY_RESULTS)
					.value(CassandraDB.COLUMN_JUSTIFICATION, resultJustification)
					.value(CassandraDB.COLUMN_ID, UUIDs.timeBased());
			sClient.getSession().execute(insert);
			
			// Added by WuGang 2015-02-14
			context.getCounter("OWL Horst Justifications Job", "ExplanationOutputs").increment(1);
		}else if (total == 0){		// Should be further traced
			for (Writable triple : key.keySet()){
//				System.out.println("find a triple to be further traced: " + (Triple)triple);
				context.write((Triple)triple, key);
			}
		}	// else do nothing.
		
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		sClient = new SimpleClientDataStax();
		sClient.connect(CassandraDB.DEFAULT_HOST);
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		sClient.close();
	}
}
