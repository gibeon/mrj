package cn.edu.neu.mitt.mrj.reasoner.rdfs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;

import com.datastax.driver.core.Row;

public class RDFSSpecialPropsMapper extends Mapper<Long, Row, BytesWritable, LongWritable> {
	
	protected static Logger log = LoggerFactory.getLogger(RDFSSpecialPropsMapper.class);

	protected LongWritable oValue = new LongWritable(0);
	byte[] bKey = new byte[17];
	protected BytesWritable oKey = new BytesWritable();
	
	protected Set<Long> memberProperties = null;
	protected Set<Long> resourceSubclasses = null;
	protected Set<Long> literalSubclasses = null;
	
	public void map(Long key, Row row, Context context) throws IOException, InterruptedException {
		Triple value = CassandraDB.readJustificationFromMapReduceRow(row);

		/*if (value.getPredicate() == TriplesUtils.RDF_TYPE) {
			if ((value.getObject() == TriplesUtils.RDFS_LITERAL || 
					literalSubclasses.contains(value.getObject()))) {
				bKey[0] = 0;
				NumberUtils.encodeNumber(bKey, 1, value.getSubject());
				oKey.set(bKey, 0, 9);
				oValue.set(value.getObject());
				output.collect(oKey, oValue);
			}
		} else */if (value.getPredicate() == TriplesUtils.RDFS_SUBPROPERTY) {
			if ((value.getObject() == TriplesUtils.RDFS_MEMBER || 
						memberProperties.contains(value.getObject()))) {
				bKey[0] = 1;
				NumberUtils.encodeLong(bKey, 1, value.getSubject());
				oKey.set(bKey, 0, 9);
				oValue.set(value.getObject());
				context.write(oKey, oValue);
			}
		} else if (value.getPredicate() == TriplesUtils.RDFS_SUBCLASS) {
			if ((value.getObject() == TriplesUtils.RDFS_LITERAL || 
						literalSubclasses.contains(value.getObject()))) {
				bKey[0] = 2;
				NumberUtils.encodeLong(bKey, 1, value.getSubject());
				oKey.set(bKey, 0, 9);
				oValue.set(value.getObject());
				context.write(oKey, oValue);
			} else if (value.getObject() == TriplesUtils.RDFS_RESOURCE || 
						resourceSubclasses.contains(value.getObject())) {
				bKey[0] = 3;
				NumberUtils.encodeLong(bKey, 1, value.getSubject());
				oKey.set(bKey, 0, 9);
				oValue.set(value.getObject());
				context.write(oKey, oValue);
			}
		} else if (memberProperties.contains(value.getPredicate()) ||
					value.getPredicate() == TriplesUtils.RDFS_MEMBER) {
			if (!value.isObjectLiteral())
				bKey[0] = 4;
			else
				bKey[0] = 5;
			NumberUtils.encodeLong(bKey, 1, value.getSubject());
			NumberUtils.encodeLong(bKey, 9, value.getObject());
			oKey.set(bKey, 0, 17);
			oValue.set(value.getPredicate());
			context.write(oKey, oValue);
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		
		try{
			CassandraDB db = new CassandraDB();
			if (memberProperties == null) {
				memberProperties = new HashSet<Long>();
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_MEMBER_SUBPROPERTY);
				db.loadSetIntoMemory(memberProperties, filters, -1);
			}
			
			if (resourceSubclasses == null) {
				resourceSubclasses = new HashSet<Long>();
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_RESOURCE_SUBCLASS);
				db.loadSetIntoMemory(resourceSubclasses, filters, -1);
			}
			
			if (literalSubclasses == null) {
				literalSubclasses = new HashSet<Long>();
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_LITERAL_SUBCLASS);
				db.loadSetIntoMemory(literalSubclasses, filters, -1);
			}
		} catch(TTransportException tte){
			tte.printStackTrace();
		} catch (InvalidRequestException e) {
			e.printStackTrace();
		} catch (UnavailableException e) {
			e.printStackTrace();
		} catch (TimedOutException e) {
			e.printStackTrace();
		} catch (SchemaDisagreementException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}
}
