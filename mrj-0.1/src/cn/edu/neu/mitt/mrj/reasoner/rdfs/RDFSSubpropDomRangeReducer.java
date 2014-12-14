package cn.edu.neu.mitt.mrj.reasoner.rdfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.data.TripleSource;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;

//public class RDFSSubpropDomRangeReducer extends Reducer<LongWritable, LongWritable, TripleSource, Triple> {
public class RDFSSubpropDomRangeReducer extends Reducer<BytesWritable, LongWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
	
	protected static Logger log = LoggerFactory.getLogger(RDFSSubpropDomRangeReducer.class);
	
	protected static Map<Long, Collection<Long>> domainSchemaTriples = null;
	protected static Map<Long, Collection<Long>> rangeSchemaTriples = null;
	protected Set<Long> propURIs = new HashSet<Long>();
//	protected Set<Long> derivedProps = new HashSet<Long>();
	protected Set<Entry<Long, Long>> derivedProps = new HashSet<Entry<Long, Long>>();	// Modified by WuGang, 2010-08-26
	private TripleSource source = new TripleSource();
	private Triple oTriple = new Triple();

	@Override
	public void reduce(BytesWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
		byte[] bKey = key.getBytes();	// Added by Wugang, 2010-08-26
//			long uri = key.get();	//对domain而言，是s；对range而言，是o
		long uri = NumberUtils.decodeLong(bKey, 0);	//对domain而言是s；对range而言是o
		long uri_opposite = NumberUtils.decodeLong(bKey, 8);	//对domain而言是o；对range而言是s

			derivedProps.clear();	//存放x
			
			//Get the predicates with a range or domain associated to this URIs
			propURIs.clear();
			Iterator<LongWritable> itr = values.iterator();
			while (itr.hasNext())
				propURIs.add(itr.next().get());	//存放p
			
			Iterator<Long> itrProp = propURIs.iterator();			
			while (itrProp.hasNext()) {
				Collection<Long> objects = null;
				long propURI = itrProp.next();
				if ((propURI & 0x1) == 1) {
					objects = rangeSchemaTriples.get(propURI >> 1);
					context.getCounter("derivation", "range matches").increment(1);
				} else {
					objects = domainSchemaTriples.get(propURI >> 1);
					context.getCounter("derivation", "domain matches").increment(1);
				}
				
				if (objects != null) {
					Iterator<Long> itr3 = objects.iterator();
					while (itr3.hasNext())
//						derivedProps.add(itr3.next());
						derivedProps.add(new AbstractMap.SimpleEntry<Long, Long>(itr3.next(), propURI));	// Modified by WuGang, 2010-08-26
				}
			}
			
			//Derive the new statements
//			Iterator<Long> itr2 = derivedProps.iterator();
			Iterator<Entry<Long, Long>> itr2 = derivedProps.iterator();	// Modified by WuGang, 2010-08-26
			oTriple.setSubject(uri);
			oTriple.setPredicate(TriplesUtils.RDF_TYPE);
			oTriple.setObjectLiteral(false);
			while (itr2.hasNext()) {				
//				oTriple.setObject(itr2.next());	
				Entry<Long, Long> entry = itr2.next();
				oTriple.setObject(entry.getKey());		// Modified by WuGang, 2010-08-26	
				// Added by WuGang, 2010-08-26
				long propURI = entry.getValue();
				oTriple.setRpredicate(propURI >> 1);	// Modified by WuGang 2010-12-03，在RDFSSubPropDomRangeMapper中左移了，现在必须再右移回来
				if ((propURI & 0x1) == 1) {	// Rule 3, for range
					oTriple.setType(TriplesUtils.RDFS_3); 
					oTriple.setRsubject(uri_opposite);
					oTriple.setRobject(uri);
				}else{	// Rule 2, for domain
					oTriple.setType(TriplesUtils.RDFS_2);
					oTriple.setRsubject(uri);
					oTriple.setRobject(uri_opposite);
				}
					
				CassandraDB.writeJustificationToMapReduceContext(oTriple, source, context);					
				//context.write(source, oTriple);
			}
			context.getCounter("RDFS derived triples", "subprop range and domain rule").increment(derivedProps.size());
	}

	@Override
	public void setup(Context context) throws IOException {
		CassandraDB.setConfigLocation();	// 2014-12-11, Very strange, this works around.

		try{
			CassandraDB db = new CassandraDB();
			if (domainSchemaTriples == null) {
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_DOMAIN_PROPERTY);
				domainSchemaTriples = db.loadMapIntoMemory(filters);
			}

			if (rangeSchemaTriples == null) {
				Set<Integer> filters = new HashSet<Integer>();
				filters.add(TriplesUtils.SCHEMA_TRIPLE_RANGE_PROPERTY);
				rangeSchemaTriples = db.loadMapIntoMemory(filters);
			}
		}catch(TTransportException tte){
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
		
		source.setDerivation(TripleSource.RDFS_DERIVED);
		source.setStep(context.getConfiguration().getInt("reasoner.step", 0));
	}
}
