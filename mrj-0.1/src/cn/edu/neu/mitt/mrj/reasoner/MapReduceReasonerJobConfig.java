/**
 * Project Name: mrj-0.1
 * File Name: MapReduceJobConfig.java
 * @author Gang Wu
 * 2014年12月28日 上午10:44:16
 * 
 * Description: 
 * TODO
 */
package cn.edu.neu.mitt.mrj.reasoner;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;

/**
 * @author gibeo_000
 *
 */
public class MapReduceReasonerJobConfig {
	
	
	// Input from CassandraDB.COLUMNFAMILY_JUSTIFICATIONS
	private static void configureCassandraInput(Job job, Set<Integer> filters) {
		//Set the input
        ConfigHelper.setInputInitialAddress(job.getConfiguration(), "localhost");
        // Should not use 9160 port in cassandra 2.1.2 because new cql3 port is 9042, please refer to conf/cassandra.yaml
        //ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");	 
        ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), CassandraDB.KEYSPACE, CassandraDB.COLUMNFAMILY_JUSTIFICATIONS);
        if (filters.size() == 0){
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
        	
        }
        else if (filters.size() == 1){
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
	        		") <= ? AND " +
	        		CassandraDB.COLUMN_TRIPLE_TYPE + " = " + filters.toArray()[0] +
	        		" ALLOW FILTERING");
        }else{
        	String strFilter = filters.toString();
        	String strInFilterClause = strFilter.substring(1, strFilter.length()-1);	// remove "[" and "]" characters of Set.toString()
        	strInFilterClause = "(" + strInFilterClause + ")";
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
            		") <= ? AND " + 
            		CassandraDB.COLUMN_TRIPLE_TYPE + " IN " + strInFilterClause +
            		" ALLOW FILTERING");        	
        }
        CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), CassandraDB.CQL_PAGE_ROW_SIZE);
        ConfigHelper.setInputSplitSize(job.getConfiguration(), 180);
        job.setInputFormatClass(CqlInputFormat.class);
	    System.out.println("ConfigHelper.getInputSplitSize - input: " + ConfigHelper.getInputSplitSize(job.getConfiguration()));
	    System.out.println("CqlConfigHelper.getInputPageRowSize - input: " + CqlConfigHelper.getInputPageRowSize(job.getConfiguration()));

	}
	
	
	// Output to CassandraDB.COLUMNFAMILY_JUSTIFICATIONS
	private static void configureCassandraOutput(Job job) {
		//Set the output
        job.setOutputKeyClass(Map.class);
        job.setOutputValueClass(List.class);
        job.setOutputFormatClass(CqlOutputFormat.class);
        ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "localhost");
        ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
        ConfigHelper.setOutputColumnFamily(job.getConfiguration(), CassandraDB.KEYSPACE, CassandraDB.COLUMNFAMILY_JUSTIFICATIONS);
        String query = "UPDATE " + CassandraDB.KEYSPACE + "." + CassandraDB.COLUMNFAMILY_JUSTIFICATIONS +
        		" SET " + CassandraDB.COLUMN_INFERRED_STEPS + "=? ";
        CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
	}

	
	// In each derivation, we may create a set of jobs
	public static Job createNewJob(Class<?> classJar, String jobName, 
			Set<Integer> filters, int numMapTasks, int numReduceTasks,
			boolean bConfigCassandraInput, boolean bConfigCassandraOutput)
		throws IOException {
		Configuration conf = new Configuration();
		conf.setInt("maptasks", numMapTasks);
		conf.set("input.filter", filters.toString());
	    
		Job job = new Job(conf);
		job.setJobName(jobName);
		job.setJarByClass(classJar);
	    job.setNumReduceTasks(numReduceTasks);
	    
	    if (bConfigCassandraInput)
	    	configureCassandraInput(job, filters);
	    if (bConfigCassandraOutput)
	    	configureCassandraOutput(job);
	    
	    // Added by WuGang 2010-05-25 
	    System.out.println("Create a job - " + jobName);
	    System.out.println("ConfigHelper.getInputSplitSize - out: " + ConfigHelper.getInputSplitSize(job.getConfiguration()));
	    System.out.println("CqlConfigHelper.getInputPageRowSize - out: " + CqlConfigHelper.getInputPageRowSize(job.getConfiguration()));

	    return job;
	}


}
