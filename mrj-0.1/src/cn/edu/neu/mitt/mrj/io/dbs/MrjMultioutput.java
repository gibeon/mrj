/**
 * 
 */
package cn.edu.neu.mitt.mrj.io.dbs;

import java.io.IOException;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlBulkOutputFormat;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * @author L
 *
 */
public class MrjMultioutput<KEYOUT, VALUEOUT> extends MultipleOutputs<KEYOUT, VALUEOUT> {

	public MrjMultioutput(TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context) {
		super(context);
	}

	@Override
	protected synchronized RecordWriter getRecordWriter(
			TaskAttemptContext taskContext, String columnFamilyNameName)
			throws IOException, InterruptedException {
		
		
//		CqlBulkOutputFormat.setColumnFamilySchema(taskContext.getConfiguration(), "step1", CassandraDB.getStepsSchema(1));
//		CqlBulkOutputFormat.setColumnFamilyInsertStatement(taskContext.getConfiguration(), "step1", CassandraDB.getAlltripleStatement());

	    // look for record-writer in the cache
	    RecordWriter writer = recordWriters.get(columnFamilyNameName);
	    
		System.out.println("get Record Writer");
	    
	    // If not in cache, create a new one
	    if (writer == null) {
	      // get the record writer from context output format
//	      FileOutputFormat.setOutputName(taskContext, baseFileName);
			System.out.println("Before ConfigHelper.setOutputColumnFamily");
			System.out.println(ConfigHelper.getOutputColumnFamily(taskContext.getConfiguration()));
	    	
	    	
			ConfigHelper.setOutputColumnFamily(taskContext.getConfiguration(), columnFamilyNameName);
			CqlConfigHelper.setOutputCql(taskContext.getConfiguration(), getCql(columnFamilyNameName));

	      try {
	    	  System.out.println(taskContext.getOutputFormatClass());
	        writer = ((OutputFormat) ReflectionUtils.newInstance(
	          taskContext.getOutputFormatClass(), taskContext.getConfiguration()))
	          .getRecordWriter(taskContext);
	        
	        System.out.println(writer.getClass());
	      } catch (ClassNotFoundException e) {
	        throw new IOException(e);
	      }
	 
	      // if counters are enabled, wrap the writer with context 
	      // to increment counters 
	      if (countersEnabled) {
	        writer = new MultipleOutputs.RecordWriterWithCounter(writer, columnFamilyNameName, context);
	      }
	      
	      // add the record-writer to the cache
	      recordWriters.put(columnFamilyNameName, writer);
	    }
	    return writer;
	}

	
	String getCql(String columnFamilyNameName){
		if (columnFamilyNameName == "alltriples") {
			System.out.println("get cql allt");
			return ("UPDATE alltriples SET inferredsteps =? , isliteral =? , tripletype =?");
		}
		System.out.println("get cql step");
		return("UPDATE " + columnFamilyNameName + " SET transitivelevel =? ");
	}
	
}
