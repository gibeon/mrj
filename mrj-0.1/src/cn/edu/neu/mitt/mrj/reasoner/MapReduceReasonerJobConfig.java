/**
 * Project Name: mrj-0.1
 * File Name: MapReduceJobConfig.java
 * @author Gang Wu
 * 2014��12��28�� ����10:44:16
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
	private static void configureCassandraInput(Job job, Set<Integer> typeFilters, Set<Integer> stepFilters) {
		//Set the input
        ConfigHelper.setInputInitialAddress(job.getConfiguration(), cn.edu.neu.mitt.mrj.utils.Cassandraconf.host);
        // Should not use 9160 port in cassandra 2.1.2 because new cql3 port is 9042, please refer to conf/cassandra.yaml
        //ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");	 
        ConfigHelper.setInputPartitioner(job.getConfiguration(), cn.edu.neu.mitt.mrj.utils.Cassandraconf.partitioner);
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), CassandraDB.KEYSPACE, CassandraDB.COLUMNFAMILY_JUSTIFICATIONS);
        if (typeFilters.size() == 0){
        	
        	if (stepFilters.size() == 0)
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
        	else{
            	Integer max = java.util.Collections.max(stepFilters);
            	Integer min = java.util.Collections.min(stepFilters);

            	
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
    	        		CassandraDB.COLUMN_STEP + " >= " + min + " AND " +
    	        		CassandraDB.COLUMN_STEP + " <= " + max +
    	        		" ALLOW FILTERING");
        	}
        		
        	
        }
        else if (typeFilters.size() == 1){
        	if (stepFilters.size() != 0){	// stepFilter is only for handling transitive property
        		System.err.println("This is not supported!!!");
        		return;
        	}
        	
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
	        		CassandraDB.COLUMN_TRIPLE_TYPE + " = " + typeFilters.toArray()[0] +
	        		" ALLOW FILTERING");
        }else{
        	if (stepFilters.size() != 0){	// stepFilter is only for handling transitive property
        		System.err.println("This is not supported!!!");
        		return;
        	}

        	
        	// The support of IN clause in cassandra db's SELECT is restricted. 
        	// So we have to try to manually cluster the values in the filters.
        	//     see http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/select_r.html#reference_ds_d35_v2q_xj__selectIN
        	System.out.println("<<<<<<<<The support of IN clause in cassandra db's SELECT is restricted.>>>>>>>>>");
        	System.out.println("<<<<<<<<So we have to try to manually cluster the values in the filters.>>>>>>>>>");
        	
        	Integer max = java.util.Collections.max(typeFilters);
        	Integer min = java.util.Collections.min(typeFilters);

        	
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
	        		CassandraDB.COLUMN_TRIPLE_TYPE + " >= " + min + " AND " +
	        		CassandraDB.COLUMN_TRIPLE_TYPE + " <= " + max +
	        		" ALLOW FILTERING");
        	
//        	String strFilter = filters.toString();
//        	String strInFilterClause = strFilter.substring(1, strFilter.length()-1);	// remove "[" and "]" characters of Set.toString()
//        	strInFilterClause = "(" + strInFilterClause + ")";
//            CqlConfigHelper.setInputCql(job.getConfiguration(), 
//            		"SELECT * FROM " + CassandraDB.KEYSPACE + "." + CassandraDB.COLUMNFAMILY_JUSTIFICATIONS + 
//            		" WHERE TOKEN(" + 
//            		CassandraDB.COLUMN_SUB + ", " + 
//            		CassandraDB.COLUMN_PRE + ", " + 
//            		CassandraDB.COLUMN_OBJ + ", " + 
//            		CassandraDB.COLUMN_IS_LITERAL +
//            		") > ? AND TOKEN(" + 
//            		CassandraDB.COLUMN_SUB + ", " + 
//            		CassandraDB.COLUMN_PRE + ", " + 
//    				CassandraDB.COLUMN_OBJ + ", " + 
//            		CassandraDB.COLUMN_IS_LITERAL + 
//            		") <= ? AND " + 
//            		CassandraDB.COLUMN_TRIPLE_TYPE + " IN " + strInFilterClause +
//            		" ALLOW FILTERING");        
            
            
        }
        CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), CassandraDB.CQL_PAGE_ROW_SIZE);
        //Modifide by LiYang  
        ConfigHelper.setInputSplitSize(job.getConfiguration(), 10000000);
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
        ConfigHelper.setOutputInitialAddress(job.getConfiguration(), cn.edu.neu.mitt.mrj.utils.Cassandraconf.host);
        ConfigHelper.setOutputPartitioner(job.getConfiguration(), cn.edu.neu.mitt.mrj.utils.Cassandraconf.partitioner);

        ConfigHelper.setOutputColumnFamily(job.getConfiguration(), CassandraDB.KEYSPACE, CassandraDB.COLUMNFAMILY_JUSTIFICATIONS);
        String query = "UPDATE " + CassandraDB.KEYSPACE + "." + CassandraDB.COLUMNFAMILY_JUSTIFICATIONS +
        		" SET " + CassandraDB.COLUMN_INFERRED_STEPS + "=? ";
        CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
	}

	
	// In each derivation, we may create a set of jobs
	public static Job createNewJob(Class<?> classJar, String jobName, 
			Set<Integer> typeFilters, Set<Integer> stepFilters, int numMapTasks, int numReduceTasks,
			boolean bConfigCassandraInput, boolean bConfigCassandraOutput)
		throws IOException {
		Configuration conf = new Configuration();
		conf.setInt("maptasks", numMapTasks);
		conf.set("input.filter", typeFilters.toString());
	    
		Job job = new Job(conf);
		job.setJobName(jobName);
		job.setJarByClass(classJar);
	    job.setNumReduceTasks(numReduceTasks);
	    
	    if (bConfigCassandraInput)
	    	configureCassandraInput(job, typeFilters, stepFilters);
	    if (bConfigCassandraOutput)
	    	configureCassandraOutput(job);
	    
	    // Added by WuGang 2010-05-25 
	    System.out.println("Create a job - " + jobName);
	    System.out.println("ConfigHelper.getInputSplitSize - out: " + ConfigHelper.getInputSplitSize(job.getConfiguration()));
	    System.out.println("CqlConfigHelper.getInputPageRowSize - out: " + CqlConfigHelper.getInputPageRowSize(job.getConfiguration()));

	    return job;
	}


}
