package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;

import com.datastax.driver.core.Row;

public class OWLNotRecursiveMapper extends Mapper<Long, Row, BytesWritable, LongWritable> {
	
	protected static Logger log = LoggerFactory.getLogger(OWLNotRecursiveMapper.class);

	protected Set<Long> schemaFunctionalProperties = null;
	protected Set<Long> schemaInverseFunctionalProperties = null;
	protected Set<Long> schemaSymmetricProperties = null;
	protected Set<Long> schemaInverseOfProperties = null;
	protected Set<Long> schemaTransitiveProperties = null;
	private byte[] bKeys = new byte[17];
	private BytesWritable key = new BytesWritable(bKeys);
	private int previousTransDerivation = -1;
	private int previousDerivation = -1;
	private boolean hasSchemaChanged = false;

	public void map(Long key, Row row, Context context) throws IOException,InterruptedException {
		int step = row.getInt(CassandraDB.COLUMN_INFERRED_STEPS);
		Triple value = CassandraDB.readJustificationFromMapReduceRow(row);
		
		/* Check if the triple has the functional property. If yes output
		 * a key value so it can be matched in the reducer.
		 */
		if (schemaFunctionalProperties.contains(value.getPredicate())
				&& !value.isObjectLiteral()) {			
			//Set as key a particular flag plus the predicate
			bKeys[0] = 0;
			NumberUtils.encodeLong(bKeys, 1, value.getSubject());
			NumberUtils.encodeLong(bKeys, 9, value.getPredicate());
			context.write(this.key, new LongWritable(value.getObject()));
		}
		
		if (schemaInverseFunctionalProperties.contains(value.getPredicate())
				&& !value.isObjectLiteral()) {
			//Set as key a particular flag plus the predicate
//			bKeys[0] = 0;
			bKeys[0] = 1;	// It should not be 0, but be 1
			NumberUtils.encodeLong(bKeys, 1, value.getObject());
			NumberUtils.encodeLong(bKeys, 9, value.getPredicate());
			context.write(this.key, new LongWritable(value.getSubject()));
		}
		
		if (schemaSymmetricProperties.contains(value.getPredicate())) {
			if (hasSchemaChanged || 
					(!hasSchemaChanged && step > previousDerivation)) {
				bKeys[0] = 2;
				NumberUtils.encodeLong(bKeys, 1, value.getSubject());
				NumberUtils.encodeLong(bKeys, 9, value.getObject());
				context.write(this.key, new LongWritable(value.getPredicate()));
			}
		}
		
		if (schemaInverseOfProperties.contains(value.getPredicate())) {
			if (hasSchemaChanged || 
					(!hasSchemaChanged && step > previousDerivation)) {		
				bKeys[0] = 3;
				NumberUtils.encodeLong(bKeys, 1, value.getSubject());
				NumberUtils.encodeLong(bKeys, 9, value.getObject());
				context.write(this.key, new LongWritable(value.getPredicate()));
			}
		}
		
		if (schemaTransitiveProperties.contains(value.getPredicate())) {
			if (!value.isObjectLiteral())
				bKeys[0] = 4;
			else
				bKeys[0] = 5;			
			NumberUtils.encodeLong(bKeys, 1, value.getSubject());
			NumberUtils.encodeLong(bKeys, 9, value.getObject());
			//Encode whether the triple is enabled or disabled
			long predicate = value.getPredicate();
			if (step <= previousTransDerivation) {
				predicate *= -1;
			} else {
				context.getCounter("OWL derived triples","new transitivity triples").increment(1);
			}
			context.write(this.key, new LongWritable(predicate));
		}
	}

	protected void setup(Context context) throws IOException {
		previousTransDerivation = context.getConfiguration().getInt("reasoner.previosTransitiveDerivation", -1);
		previousDerivation = context.getConfiguration().getInt("reasoner.previousDerivation", -1);
		hasSchemaChanged = false;
		
		try{
			CassandraDB db = new CassandraDB();
	
			if (schemaFunctionalProperties == null) {
				schemaFunctionalProperties = new HashSet<Long>();
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_FUNCTIONAL_PROPERTY);
				hasSchemaChanged = db.loadSetIntoMemory(schemaFunctionalProperties, filters, previousDerivation);
			}
			
			if (schemaInverseFunctionalProperties == null) {
				schemaInverseFunctionalProperties = new HashSet<Long>();
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_INVERSE_FUNCTIONAL_PROPERTY);
				hasSchemaChanged = db.loadSetIntoMemory(schemaInverseFunctionalProperties, filters, previousDerivation);
			}
			
			if (schemaSymmetricProperties == null) {
				schemaSymmetricProperties = new HashSet<Long>();
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_SYMMETRIC_PROPERTY);
				hasSchemaChanged = db.loadSetIntoMemory(schemaSymmetricProperties, filters, previousDerivation);
			}
			
			if (schemaInverseOfProperties == null) {
				schemaInverseOfProperties = new HashSet<Long>();
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_INVERSE_OF);
				hasSchemaChanged = db.loadSetIntoMemory(schemaInverseOfProperties, filters, previousDerivation);
			}
			
			if (schemaTransitiveProperties == null) {
				schemaTransitiveProperties = new HashSet<Long>();
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_TRANSITIVE_PROPERTY);
				hasSchemaChanged = db.loadSetIntoMemory(schemaTransitiveProperties, filters, previousDerivation);
			}
		}catch(TException te){
			te.printStackTrace();
		}
	}
}
