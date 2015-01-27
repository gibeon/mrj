package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;
import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.data.TripleSource;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;

public class OWLTransitivityReducer extends Reducer<BytesWritable, BytesWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {

	protected static Logger log = LoggerFactory.getLogger(OWLTransitivityReducer.class);
	
	private TripleSource source = new TripleSource();
	private Triple triple = new Triple();
	private HashMap<Long,Long> firstSet = new HashMap<Long,Long>();
	private HashMap<Long,Long> secondSet = new HashMap<Long,Long>();
	
	private int baseLevel = 0;
	private int level = 0;

	public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
		
		firstSet.clear();
		secondSet.clear();
		
		Iterator<BytesWritable> itr = values.iterator();
		while (itr.hasNext()) {
			byte[] value = itr.next().getBytes();
			long level = NumberUtils.decodeLong(value, 1);
			long resource = NumberUtils.decodeLong(value, 9);
			if (value[0] == 0 || value[0] == 1) {
				if (!firstSet.containsKey(resource) || 
						(firstSet.containsKey(resource) && firstSet.get(resource) < level)) {
					if (value[0] == 1) {
						firstSet.put(resource, level * -1);						
					} else {
						firstSet.put(resource, level);
					}
				}
			} else {
				if (!secondSet.containsKey(resource) || 
						(secondSet.containsKey(resource) && secondSet.get(resource) < level)) {
					if (value[0] == 3) {
						secondSet.put(resource, level * -1);						
					} else {
						secondSet.put(resource, level);
					}
				}
			}
		}
		
//		System.out.println("In OWLTransitivityReducer, firstSet size is " + firstSet.size() + ", secondSet size is " + secondSet.size());
		
		if (firstSet.size() == 0 || secondSet.size() == 0)
			return;
		
		triple.setPredicate(NumberUtils.decodeLong(key.getBytes(),0));
		
		// Added by WuGang,针对extended triple，设置为u p w,其中w是一个关键的resource，用于重构原始的rule前件
		triple.setType(TriplesUtils.OWL_HORST_4);
//		triple.setRsubject(rsubject);	// 这个是依赖于推理结果的，请参见下面的代码
		triple.setRpredicate(NumberUtils.decodeLong(key.getBytes(),0));
		triple.setRobject(NumberUtils.decodeLong(key.getBytes(), 8));
		
		Iterator<Entry<Long, Long>> firstItr = firstSet.entrySet().iterator();
		context.getCounter("stats", "joinPoint").increment(1);
		while (firstItr.hasNext()) {
			Entry<Long,Long> entry = firstItr.next();
			Iterator<Entry<Long, Long>> secondItr = secondSet.entrySet().iterator();
			while (secondItr.hasNext()) {
				Entry<Long,Long> entry2 = secondItr.next();
				//Output the triple
				if (level != 1 || (level == 1 && (entry.getValue() > 0 || entry2.getValue() > 0))) { //TODO:
					triple.setSubject(entry.getKey());
					triple.setObject(entry2.getKey());
					
					// Added by Wugang, 针对extended triple，实际上这个rsubject设不设置都无所谓，但为了保持完整还是设置吧
					triple.setRsubject(triple.getSubject());	// 因为是选取u p w作为相关triple，因此其中的u就是最终推理的后件的主语
					
					source.setStep((int)(Math.abs(entry.getValue()) + Math.abs(entry2.getValue()) - baseLevel));
					
//					context.write(source, triple);
					CassandraDB.writeJustificationToMapReduceContext(triple, source, context);
					
//					System.out.println("In OWLTransitivityReducer: " + triple);
				}
			}
		}
	}

	@Override
	public void setup(Context context) {
		CassandraDB.setConfigLocation();	// 2014-12-11, Very strange, this works around.

		baseLevel = context.getConfiguration().getInt("reasoning.baseLevel", 1) - 1;
		level = context.getConfiguration().getInt("reasoning.transitivityLevel", -1);
		// Modified by WuGang 2015-01-28
		//source.setDerivation(TripleSource.OWL_DERIVED);	
		source.setDerivation(TripleSource.TRANSITIVE_ENABLED);
		triple.setObjectLiteral(false);
	}
}
