package prejustification;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.reasoner.MapReduceReasonerJobConfig;


public class SelectInferRows extends Configured implements Tool{
	//private static final Logger logger = LoggerFactory.getLogger();
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new Configuration(), new SelectInferRows(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception{

		//Job job = new Job(getConf());
//		Job job = MapReduceReasonerJobConfig.createNewJob(SelectInferRows.class, "Select Rows", new HashSet<Integer>(), 16, 16, true, true);
		
//		ConfigHelper.setInputInitialAddress(getConf(), CassandraDB.DEFAULT_HOST);
//		ConfigHelper.setInputColumnFamily(getConf(), CassandraDB.KEYSPACE, CassandraDB.COLUMN_JUSTIFICATION);

		//		job.setJobName("Del Rows");
//		job.setJarByClass(SelectInferRows.class);	   
		
		/*
		 * Select(map)
		 */
		
		
		Configuration conf = new Configuration();
	    
		Job job = new Job(conf);
		job.setJobName(" Test ");
		job.setJarByClass(SelectInferRows.class);
	    job.setNumReduceTasks(8);
	    
        ConfigHelper.setInputInitialAddress(job.getConfiguration(), cn.edu.neu.mitt.mrj.utils.Cassandraconf.host);
        ConfigHelper.setInputPartitioner(job.getConfiguration(), cn.edu.neu.mitt.mrj.utils.Cassandraconf.partitioner);
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), CassandraDB.KEYSPACE, CassandraDB.COLUMNFAMILY_JUSTIFICATIONS);
	        CqlConfigHelper.setInputCql(job.getConfiguration(), 
	        		"SELECT * FROM " + CassandraDB.KEYSPACE + "." + CassandraDB.COLUMNFAMILY_JUSTIFICATIONS + 
	        		" WHERE TOKEN(" + 
	        		CassandraDB.COLUMN_SUB + ", " + 
	        		CassandraDB.COLUMN_PRE + ", " + 
	        		CassandraDB.COLUMN_OBJ + ", " + 
	        		CassandraDB.COLUMN_IS_LITERAL +
	        		") > ? AND TOKEN(" + 
	        		CassandraDB.COLUMN_SUB + ", " + 
	        		CassandraDB.COLUMN_PRE + ", " + 
					CassandraDB.COLUMN_OBJ + ", " + 
	        		CassandraDB.COLUMN_IS_LITERAL + 
	        		") <= ? ALLOW FILTERING");
	        CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), CassandraDB.CQL_PAGE_ROW_SIZE);
	        //Modifide by LiYang  
	        ConfigHelper.setInputSplitSize(job.getConfiguration(), 10000000);
	        job.setInputFormatClass(CqlInputFormat.class);
	        job.setOutputKeyClass(Map.class);
	        job.setOutputValueClass(List.class);
	        job.setOutputFormatClass(CqlOutputFormat.class);
	        ConfigHelper.setOutputInitialAddress(job.getConfiguration(), cn.edu.neu.mitt.mrj.utils.Cassandraconf.host);
	        ConfigHelper.setOutputPartitioner(job.getConfiguration(), cn.edu.neu.mitt.mrj.utils.Cassandraconf.partitioner);

	        ConfigHelper.setOutputColumnFamily(job.getConfiguration(), CassandraDB.KEYSPACE, CassandraDB.COLUMNFAMILY_JUSTIFICATIONS);
	        String query = "UPDATE " + CassandraDB.KEYSPACE + "." + CassandraDB.COLUMNFAMILY_JUSTIFICATIONS +
	        		" SET " + CassandraDB.COLUMN_INFERRED_STEPS + "=? ";
	        CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
	        
		job.setMapperClass(SelectInferRowsMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ByteBuffer.class);
	    job.setReducerClass(SelectInferRowsReduce.class);

	    
//		job.setInputFormatClass(ColumnFamilyInputFormat.class);	
//		ConfigHelper.setInputInitialAddress(job.getConfiguration(), cn.edu.neu.mitt.mrj.utils.Cassandraconf.host);	     
//		ConfigHelper.setInputPartitioner(job.getConfiguration(), cn.edu.neu.mitt.mrj.utils.Cassandraconf.partitioner);     
//		ConfigHelper.setInputColumnFamily(job.getConfiguration(), CassandraDB.KEYSPACE, CassandraDB.COLUMNFAMILY_JUSTIFICATIONS);
//        CqlConfigHelper.setInputCql(job.getConfiguration(), 
//        		"SELECT * FROM " + CassandraDB.KEYSPACE + "." + CassandraDB.COLUMNFAMILY_JUSTIFICATIONS + 
//        		" WHERE RULE = 0");
//        CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), CassandraDB.CQL_PAGE_ROW_SIZE);
//        ConfigHelper.setInputSplitSize(job.getConfiguration(), 10000000);
//        job.setInputFormatClass(CqlInputFormat.class);
//
//		
//		/*
//		 * Insert(reduce)
//		 */
////		job.setCombinerClass(SelectInferRowsReduce.class);
//		job.setOutputKeyClass(Map.class);
//		job.setOutputValueClass(List.class); 	
//		//相当于 指定输出目录 要写上 否则会提示找不到输出目录
//		job.setOutputFormatClass(CqlOutputFormat.class);
//		
//	    ConfigHelper.setOutputInitialAddress(job.getConfiguration(), CassandraDB.DEFAULT_HOST);
//	    ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
//        ConfigHelper.setOutputColumnFamily(getConf(), CassandraDB.KEYSPACE, "ruleiszero");
//        ConfigHelper.setOutputKeyspace(job.getConfiguration(), CassandraDB.KEYSPACE);// **
////       String query = "INSERT INTO mrjks.ruleiszero (" + CassandraDB.COLUMN_SUB + ", " + CassandraDB.COLUMN_PRE + ", " + CassandraDB.COLUMN_OBJ +  ", " + CassandraDB.COLUMN_IS_LITERAL + ", " +
////                CassandraDB.COLUMN_TRIPLE_TYPE + ", " + CassandraDB.COLUMN_RULE + ", " + CassandraDB.COLUMN_V1 + ", " + CassandraDB.COLUMN_V2 + ", " +  CassandraDB.COLUMN_V3 +", "
////                + CassandraDB.COLUMN_INFERRED_STEPS + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, )";
//      String query =  "UPDATE " + CassandraDB.KEYSPACE + "." + CassandraDB.COLUMNFAMILY_JUSTIFICATIONS +
//      		" SET " + CassandraDB.COLUMN_INFERRED_STEPS + "=? "; 
//      CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
//
//  
//      ConfigHelper.getInputSplitSize(job.getConfiguration());
//      CqlConfigHelper.getInputPageRowSize(job.getConfiguration());
////      String column_names =  CassandraDB.COLUMN_SUB + CassandraDB.COLUMN_PRE + CassandraDB.COLUMN_OBJ + CassandraDB.COLUMN_IS_LITERAL +
////            CassandraDB.COLUMN_TRIPLE_TYPE  + CassandraDB.COLUMN_RULE  + CassandraDB.COLUMN_V1  + CassandraDB.COLUMN_V2  +  CassandraDB.COLUMN_V3;
////      SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes(column_names)));
//        
//      //必须有这句，否则不能启动map 和 reduce
        job.waitForCompletion(true);
		
		System.out.println("Finished");
		return 0;
		
	}
	
}
