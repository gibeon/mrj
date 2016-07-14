package cn.edu.neu.mitt.mrj.reasoner.owl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import cn.edu.neu.mitt.mrj.utils.NumberUtils;

public class OWLSameAsDeconstructReducer extends Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable> {

	private LongWritable oKey = new LongWritable();
	private BytesWritable oValue = new BytesWritable();
	
	private byte[] bOriginalResource = new byte[8];	//多一个Long用于存放原始（替换前）的resource

	
	private List<byte[]> storage = new LinkedList<byte[]>();
	
	@Override
	public void reduce(LongWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {		
//		System.out.println("In OWLSameAsDeconstructReducer!!!");

		storage.clear();
		long countOutput = 0;
		oKey.set(key.get());
		NumberUtils.encodeLong(bOriginalResource, 0, key.get());	// Added by Wugang
		
		boolean replacement = false;
		Iterator<BytesWritable> itr = values.iterator();
		while (itr.hasNext()) {
			BytesWritable iValue = itr.next();
			byte[] bValue = iValue.getBytes();
//			System.out.println("In processing things before storage, size of iValue is: " + iValue.getLength());
//			System.out.println("In processing things before storage, size of bValue is: " + bValue.length);
			// 由于不一定在何时才能遇到bValue[0]=4的那个key和value，
			// 因此在遇到之前先将待替换的resource都放在storage里保存起来，而一旦遇到了之后就可以直接进行替换了，用value替换
			// 最后(在这个while循环之外)再把storage里面保存的值重新替换一下。
			if (bValue[0] == 4) {//Same as
				long resource = NumberUtils.decodeLong(bValue, 1);
				replacement = true;
				oKey.set(resource);
			} else if (!replacement && bValue[14] == 0) {
				//Store in memory the results
				byte[] newValue = Arrays.copyOf(bValue, bValue.length);
				storage.add(newValue);
			} else {
				//I already found the replacement. Simply output everything
				byte[] bTempValue = new byte[15+8];	// Added by WuGang
				System.arraycopy(bValue, 0, bTempValue, 0, 15);	// Added by WuGang
				System.arraycopy(bOriginalResource, 0, bTempValue, 15, 8);	// Added by WuGang
				iValue.set(bTempValue, 0, bTempValue.length);	// Added by Wugang, 在最后写入一个替换前的resource
				context.write(oKey, iValue);
				countOutput++;
				context.getCounter("reasoner", "substitutions").increment(1);
			}
		}
		
		Iterator<byte[]> itr2 = storage.iterator();	//如果storage为空则说明，在这个机器上没有可以用sameas来替换的东西
		while (itr2.hasNext()) {
			byte[] bValue = itr2.next();
			oValue.set(bValue, 0, bValue.length);
			
			byte[] bTempValue = new byte[15+8];	// Added by WuGang
//			System.out.println("In processing things in storage, size of bValue is: " + bValue.length);
			System.arraycopy(bValue, 0, bTempValue, 0, 15);	// Added by WuGang
			System.arraycopy(bOriginalResource, 0, bTempValue, 15, 8);	// Added by WuGang
			oValue.set(bTempValue, 0, bTempValue.length);	// Added by Wugang, 在最后写入一个替换前的resource
			context.write(oKey, oValue);
		}
		
		//输出的oKey是一个资源，就是sameas的主语，其他的都被替换掉了（其实就是已经都被sameas替换后的了）
		//输出的oVlaue是owl:sameas三元组的主语，或者是tripleId+key.getStep()+key.getDerivation()，前面还有一个byte
		
		if (replacement) { //Increment counter of replacements
			context.getCounter("reasoner", "substitutions").increment(countOutput + storage.size());
		}
	}
}
