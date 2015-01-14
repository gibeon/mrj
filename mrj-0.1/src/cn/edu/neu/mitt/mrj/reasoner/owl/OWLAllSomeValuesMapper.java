package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.hadoop.io.BytesWritable;
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

public class OWLAllSomeValuesMapper extends Mapper<Long, Row, BytesWritable, BytesWritable> {

	private static Logger log = LoggerFactory.getLogger(OWLAllSomeValuesMapper.class);
	
	private byte[] bKey = new byte[17];
	private BytesWritable oKey = new BytesWritable(bKey);
	
	// Modified by WuGang，需要额外传送一个long来存储相关信息,一个byte来决定是someValues(0)还是allValues(1)
	private byte[] bValue = new byte[18]; //new byte[9];	
	private BytesWritable oValue = new BytesWritable(bValue);
	
	private static Map<Long, Collection<byte[]>> allValues = null;
	private static Map<Long, Collection<byte[]>> someValues = null;
	private static Set<Long> onPropertyAll = null;
	private static Set<Long> onPropertySome = null;
	private int previousDerivation = -1;
	
	public void map(Long key, Row row, Context context) throws IOException, InterruptedException {

		log.info("I'm in OWLAllSomeValuesMapper");
		int step = row.getInt(CassandraDB.COLUMN_INFERRED_STEPS);
		Triple value = CassandraDB.readJustificationFromMapReduceRow(row);

		if (value.getPredicate() == TriplesUtils.RDF_TYPE
				&& step >= previousDerivation) {
			
			log.info("And I met a triple with RDF_TYPE as predicate: " + value);
		
			// 需要额外传送一个w
			if (someValues.containsKey(value.getObject())) {	//找到了一个(x,rdf:type,w)这样的三元组，其中w满足v owl:someValuesFrom w
				log.info("I met someValuesFrom: " + value);
				Collection<byte[]> values = someValues.get(value.getObject());
				Iterator<byte[]> itr = values.iterator();
				bKey[0] = 2;
				bValue[0] = 1;
				bValue[17] = 0;	// 表明这是一个someValues
				NumberUtils.encodeLong(bKey, 9, value.getSubject());		
				NumberUtils.encodeLong(bValue, 9, value.getObject());	//Added by WuGang, 将w写入value中
				while (itr.hasNext()) {
					byte[] bytes = itr.next();
					System.arraycopy(bytes, 0, bKey, 1, 8);
					System.arraycopy(bytes, 8, bValue, 1, 8);
					context.write(oKey, oValue);	//输出的是((p,x),v) -> ((p,x),(v,w,0))
				}
			}
			
			// 需要额外传送一个v
			if (allValues.containsKey(value.getObject())) {	//找到了一个(w,rdf:type,v)这样的三元组，其中v满足v owl:allValuesFrom u
				log.info("I met allValuesFrom: " + value);
				Collection<byte[]> values = allValues.get(value.getObject());
				Iterator<byte[]> itr = values.iterator();
				bKey[0] = 1;
				bValue[0] = 1;
				bValue[17] = 1;	// 表明这是一个allValues
				NumberUtils.encodeLong(bKey, 9, value.getSubject());	
				NumberUtils.encodeLong(bValue, 9, value.getObject());	//Added by WuGang, 将v写入value中
				while (itr.hasNext()) {
					byte[] bytes = itr.next();
					System.arraycopy(bytes, 0, bKey, 1, 8);
					System.arraycopy(bytes, 8, bValue, 1, 8);
					context.write(oKey, oValue);	//输出的是((p,w),u) -> ((p,w),(u,v,1))
				}
			}

		} else {
			// onPropertySome中放置的是所有的是onPropertySome中引用的属性，即具有的 v owl:someValuesFrom w形式的三元组中的w
			if (onPropertySome.contains(value.getPredicate())) {//某一个三元组u p x中的p是一个onPropertySome中引用到的属性
				//Rule 15 - someValuesFrom
				log.info("I met onPropertySome: " + value);
				bKey[0] = 2;
				bValue[0] = 0;
				NumberUtils.encodeLong(bKey, 1, value.getPredicate());
				NumberUtils.encodeLong(bKey, 9, value.getObject());			
				NumberUtils.encodeLong(bValue, 1, value.getSubject());
				context.write(oKey, oValue);	//输出是((p,x),(u,,))	value的后两个field是任意值,没有赋值
			}
			
			// onPropertyAll中放置的是所有的是onPropertyAll中引用的属性，即具有的 v owl:allValuesFrom u形式的三元组中的u
			if (onPropertyAll.contains(value.getPredicate())) {//某一个三元组w p x中的p是一个onPropertyAll中引用到的属性
				//Rule 16 - allValuesFrom
				log.info("I met onPropertyAll: " + value);
				bKey[0] = 1;
				bValue[0] = 0;	// Added by WuGang, 原来这里少了这一行，当有多个此类型三元组时（即多个<w p x>），在reduce过程中会造成错误！
				NumberUtils.encodeLong(bKey, 1, value.getPredicate());
				NumberUtils.encodeLong(bKey, 9, value.getSubject());
				NumberUtils.encodeLong(bValue, 1, value.getObject());
				context.write(oKey, oValue);	//输出的是((p,w),(x,,))	value的后两个field是任意值,没有赋值
			}
		}
	}
	
	@Override
	public void setup(Context context) throws IOException {
		previousDerivation = context.getConfiguration().getInt("reasoner.previousDerivation", -1);
		
//		List<FileStatus> filesProperty =  MultiFilesReader.recursiveListStatus(context, "FILTER_ONLY_OWL_ON_PROPERTY");		
//		Map<Long,Collection<Long>> allValuesTmp = FilesTriplesReader.loadMapIntoMemory("FILTER_ONLY_OWL_ALL_VALUES", context);
//		Map<Long,Collection<Long>> someValuesTmp = FilesTriplesReader.loadMapIntoMemory("FILTER_ONLY_OWL_SOME_VALUES", context);
		
		CassandraDB db;
		try{
			db = new CassandraDB();
			
			Set<Integer> filters = new HashSet<Integer>();
			filters.add(TriplesUtils.SCHEMA_TRIPLE_ON_PROPERTY);
			Map<Long,Collection<Long>> onPropertyTmp = db.loadMapIntoMemory(filters);
			
			filters.clear();
			filters.add(TriplesUtils.SCHEMA_TRIPLE_ALL_VALUES_FROM);
			Map<Long,Collection<Long>> allValuesTmp = db.loadMapIntoMemory(filters);
			
			filters.clear();
			filters.add(TriplesUtils.SCHEMA_TRIPLE_SOME_VALUES_FROM);
			Map<Long,Collection<Long>> someValuesTmp = db.loadMapIntoMemory(filters);
			
			onPropertyAll = new HashSet<Long>();
			onPropertySome = new HashSet<Long>();
			someValues = new HashMap<Long, Collection<byte[]>>();
			allValues = new HashMap<Long, Collection<byte[]>>();
			makeJoin(onPropertyTmp, context, someValuesTmp, 
					allValuesTmp, someValues, allValues,
					onPropertySome, onPropertyAll);
		}catch (TTransportException e) {
			e.printStackTrace();
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
	
		log.info("After setup, allValuesFrom is: " + allValues);
	}
	
	protected void makeJoin(Map<Long,Collection<Long>> onPropertyTmp, Context context,
			Map<Long, Collection<Long>> someValuesTmp, 
			Map<Long, Collection<Long>> allValuesTmp,
			Map<Long, Collection<byte[]>> someValues,
			Map<Long, Collection<byte[]>> allValues,
			Set<Long> onPropertySome,
			Set<Long> onPropertyAll) {
//		TripleSource key = new TripleSource();
//		Triple value = new Triple();
		
		for (Entry<Long, Collection<Long>> entry : onPropertyTmp.entrySet()){
			Long sub = entry.getKey();
			Collection<Long> objects = entry.getValue();
			for (Long obj : objects){

				//Check if there is a match with someValuesFrom and allValuesFrom
				if (someValuesTmp.containsKey(sub)) {
					Collection<Long> col = someValuesTmp.get(sub);
					if (col != null) {
						Iterator<Long> itr = col.iterator();
						while (itr.hasNext()) {
							long w = itr.next();
							byte[] bytes = new byte[16];
							//Save v and p there.
							NumberUtils.encodeLong(bytes, 0, obj);
							NumberUtils.encodeLong(bytes, 8, sub);
							
							Collection<byte[]> cValues = someValues.get(w);
							if (cValues == null) {
								cValues = new ArrayList<byte[]>();
								someValues.put(w, cValues);
							}
							cValues.add(bytes);
							onPropertySome.add(obj);
						}
					}
				}
				
				if (allValuesTmp.containsKey(sub)) {
					// col中应该是所有的subject对应的object，其中subject和object满足subject, owl:allValuesFrom, object
					Collection<Long> col = allValuesTmp.get(sub);	
					if (col != null) {
						Iterator<Long> itr = col.iterator();
						while (itr.hasNext()) {
							long w = itr.next();
							byte[] bytes = new byte[16];
							//Save v and p there.
							NumberUtils.encodeLong(bytes, 0, obj);
							NumberUtils.encodeLong(bytes, 8, w);
							
							Collection<byte[]> cValues = allValues.get(sub);
							if (cValues == null) {
								cValues = new ArrayList<byte[]>();
								allValues.put(sub, cValues);
							}
							cValues.add(bytes);
							onPropertyAll.add(obj);
						}
					}
				}
			}
		}
		
	}
}
