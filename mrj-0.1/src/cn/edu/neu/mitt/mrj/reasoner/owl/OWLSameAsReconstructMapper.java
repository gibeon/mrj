package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.utils.NumberUtils;

public class OWLSameAsReconstructMapper extends Mapper<LongWritable, BytesWritable, BytesWritable, BytesWritable> {
	
	protected static Logger log = LoggerFactory.getLogger(OWLSameAsReconstructMapper.class);

	private BytesWritable oKey = new BytesWritable();
	private BytesWritable oValue = new BytesWritable();
//	private byte[] bValue = new byte[9];
	private byte[] bValue = new byte[9+8];	//多一个Long，用于保存替换前的resource

	public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
		oKey.set(value.getBytes(), 1, 13);	//是owl:sameas三元组的主语，或者是tripleId+key.getStep()+key.getDerivation()
		bValue[0] = value.getBytes()[0];	//可能的值是0,1,2,3,4，4是表示谓语是owl:sameas，其余为非owl:sameas，其中0表示主语，1表示谓语，2和3表示宾语
		
		long resource = NumberUtils.decodeLong(value.getBytes(), 15);	// Added by Wugang, 从value中获得未替换前的resource，第14个byte扔掉
		
		NumberUtils.encodeLong(bValue, 1, key.get());	//这里从1字节开始就是owlsameas的主语，而0字节变成了用于表明待替换的位置（可能的值是0,1,2,3,4，4是表示谓语是owl:sameas，其余为非owl:sameas，其中0表示主语，1表示谓语，2和3表示宾语）
		
		NumberUtils.encodeLong(bValue, 9, resource);	// Added by Wugang, 写入替换前的resource到oValue中
		
		// 实际上是做了个，key和value的交换，因为之前value中保存的是三元组的id，交换后就可以作为key了
		// 这样就可以在reduce过程中得到关于一个三元组（用key中的id标识）的完全替换后的新三元组了
		context.write(oKey, oValue);
	}
	
	@Override
	public void setup(Context context) {
		oValue = new BytesWritable(bValue);
	}
}
