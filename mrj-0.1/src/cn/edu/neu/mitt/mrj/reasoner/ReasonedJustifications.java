package cn.edu.neu.mitt.mrj.reasoner;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import prejustification.SelectInferRows;
import prejustification.SelectInferRowsMap;
import prejustification.SelectInferRowsReduce;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;
import cn.edu.neu.mitt.mrj.utils.Cassandraconf;

public class ReasonedJustifications extends Configured implements Tool{
	public int run(String[] args) throws Exception{	
		
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
	        String query = "UPDATE " + CassandraDB.KEYSPACE + "." + "resultrows" +
	        		" SET " + CassandraDB.COLUMN_INFERRED_STEPS + "=? ";
	        CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
	        
		job.setMapperClass(ReasonedJustificationsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
	    job.setReducerClass(ReasonedJustificationsReducer.class);
		
	    
//		Configuration conf = getConf();
//		Job job = new Job(conf, "Select Reasoned Rows");
//		job.setJarByClass(ReasonedJustifications.class);
//		/*
//		//Set the predicate
//		List<ByteBuffer> columnNames = new ArrayList<ByteBuffer>();
//		columnNames.add(ByteBufferUtil.bytes(CassandraDB.COLUMN_SUB));
//		columnNames.add(ByteBufferUtil.bytes(CassandraDB.COLUMN_PRE));
//		columnNames.add(ByteBufferUtil.bytes(CassandraDB.COLUMN_PRE));
//		columnNames.add(ByteBufferUtil.bytes(CassandraDB.COLUMN_IS_LITERAL));
//		columnNames.add(ByteBufferUtil.bytes(CassandraDB.COLUMN_TRIPLE_TYPE));
//		columnNames.add(ByteBufferUtil.bytes(CassandraDB.COLUMN_RULE));
//		columnNames.add(ByteBufferUtil.bytes(CassandraDB.COLUMN_V1));
//		columnNames.add(ByteBufferUtil.bytes(CassandraDB.COLUMN_V2));
//		columnNames.add(ByteBufferUtil.bytes(CassandraDB.COLUMN_V3));
//		columnNames.add(ByteBufferUtil.bytes(CassandraDB.COLUMN_INFERRED_STEPS));
//		SlicePredicate predicate = new SlicePredicate().setColumn_names(columnNames);
//        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);
//        */
//		/*
//		 * Get Attention conf != job.getConfiguration()
//		 * thread "main" java.lang.NullPointerException at org.apache.cassandra.utils.FBUtilities.newPartitioner(FBUtilities.java:418)
//		 */
//		//Input
//
//		ConfigHelper.setInputInitialAddress(job.getConfiguration(), cn.edu.neu.mitt.mrj.utils.Cassandraconf.host);
//		ConfigHelper.setInputPartitioner(job.getConfiguration(), Cassandraconf.partitioner);
//		ConfigHelper.setInputColumnFamily(job.getConfiguration(), CassandraDB.KEYSPACE, CassandraDB.COLUMN_JUSTIFICATION);
//		CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "1000");
//
//
//		
//		CqlConfigHelper.setInputCql(job.getConfiguration(), "SELECT * FROM mrjks.justifications WHERE TOKEN(" + 
//        		CassandraDB.COLUMN_SUB + ", " + 
//        		CassandraDB.COLUMN_PRE + ", " + 
//        		CassandraDB.COLUMN_OBJ + ", " + 
//        		CassandraDB.COLUMN_IS_LITERAL +
//        		") > ? AND TOKEN(" + 
//        		CassandraDB.COLUMN_SUB + ", " + 
//        		CassandraDB.COLUMN_PRE + ", " + 
//				CassandraDB.COLUMN_OBJ + ", " + 
//        		CassandraDB.COLUMN_IS_LITERAL + 
//        		") <= ? AND " +
//        		CassandraDB.COLUMN_TRIPLE_TYPE + " = " + "5" +
//        		" ALLOW FILTERING;");
//		ConfigHelper.setInputSplitSize(job.getConfiguration(), 10000000);
//		job.setInputFormatClass(ColumnFamilyInputFormat.class);
//
//		
//		//output
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//		job.setOutputFormatClass(CqlOutputFormat.class);
//		ConfigHelper.setOutputInitialAddress(job.getConfiguration(), CassandraDB.DEFAULT_HOST);
//		ConfigHelper.setOutputPartitioner(job.getConfiguration(), Cassandraconf.partitioner);     
//		
//		ConfigHelper.setOutputColumnFamily(job.getConfiguration(), CassandraDB.KEYSPACE, CassandraDB.COLUMNFAMILY_JUSTIFICATIONS);
//
//		//ConfigHelper.setOutputKeyspace(job.getConfiguration(), CassandraDB.KEYSPACE);	//***
//		String query = "UPDATE mrjks.resultrows SET " + CassandraDB.COLUMN_INFERRED_STEPS + "= ?";
//		CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
//
//
//		job.setMapperClass(ReasonedJustificationsMapper.class); 
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(IntWritable.class);
//		job.setReducerClass(ReasonedJustificationsReducer.class);
	
		
		job.waitForCompletion(true);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ReasonedJustifications(), args);
		System.exit(res);
	}
	
}