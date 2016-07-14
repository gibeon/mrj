package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.NumberUtils;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;
import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.data.TripleSource;

public class OWLSameAsReconstructReducer extends Reducer<BytesWritable, BytesWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {

	private TripleSource oKey = new TripleSource();
	private Triple oValue = new Triple();
	
	@Override
	public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
//		System.out.println("In OWLSameAsReconstructReducer!!!");
		byte[] bKey = key.getBytes();		
		oKey.setStep(NumberUtils.decodeInt(bKey, 8));
		oKey.setDerivation(bKey[12]);
		
		int elements = 0;
		Iterator<BytesWritable> itr = values.iterator();	////这里从1字节开始就是owlsameas的主语，而0字节变成了用于表明待替换的位置（可能的值是0,1,2,3,4，4是表示谓语是owl:sameas，其余为非owl:sameas，其中0表示主语，1表示谓语，2和3表示宾语）
		while (itr.hasNext()) {
			elements++;
			byte[] bValue = itr.next().getBytes();
			long resource = NumberUtils.decodeLong(bValue, 1);	//这个就是owlsameas的主语，下面将用它进行替换了
			long originalResource = NumberUtils.decodeLong(bValue, 9);	// Added by Wugang, 读出替换前的resource
			switch (bValue[0]) {
				case 0:
					oValue.setSubject(resource);	//替换主语
					oValue.setRsubject(originalResource);	// Added by Wugang, 原始主语
//					System.out.println("Replacing subject: "  + resource);
					break;
				case 1:
					oValue.setPredicate(resource);	//替换谓语
					oValue.setRpredicate(originalResource);	// Added by Wugang, 原始谓语
//					System.out.println("Replacing predicate: "  + resource);
					break;
				case 2:								//替换宾语
				case 3:								//替换宾语
					if (bValue[0] == 2)
						oValue.setObjectLiteral(false);
					else
						oValue.setObjectLiteral(true);
					oValue.setObject(resource);			
					oValue.setRobject(originalResource);	// Added by Wugang, 原始宾语
//					System.out.println("Replacing object: "  + resource);
					break;
				default:
					break;
			}
		}
		
		if (elements == 3){
			// Added by WuGang, 针对rule11
//			oValue.setRsubject(rsubject)
			if ((oValue.getSubject() == oValue.getRsubject())
					&& (oValue.getPredicate() == oValue.getRpredicate())
					&& (oValue.getObject() == oValue.getRobject()))
				oValue.setType(TriplesUtils.OWL_HORST_NA); // 替换前后没有变化，则不是一个sameasrule
			else {

				if ((oValue.getPredicate() == TriplesUtils.OWL_SAME_AS)
						&& (oValue.getRpredicate() == TriplesUtils.OWL_SAME_AS))
					oValue.setType(TriplesUtils.OWL_HORST_7); // 应用OWL Horst规则7
				else
					oValue.setType(TriplesUtils.OWL_HORST_11); // 应用OWL
																// Horst规则11
			}
			
//			System.out.println("Find a complete replacment of triple: " + oValue);
			CassandraDB.writeJustificationToMapReduceContext(oValue, oKey, context);
//			context.write(oKey, oValue);
		}
	}
	
	@Override
	public void setup(Context context) throws IOException {
		CassandraDB.setConfigLocation();	// 2014-12-11, Very strange, this works around.
	}
}
