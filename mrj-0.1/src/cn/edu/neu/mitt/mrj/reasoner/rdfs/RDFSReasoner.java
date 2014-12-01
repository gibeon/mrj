package cn.edu.neu.mitt.mrj.reasoner.rdfs;


import java.io.IOException;
import java.util.List;
import java.util.Map;

import cn.edu.neu.mitt.mrj.partitioners.MyHashPartitioner;
import cn.edu.neu.mitt.mrj.reasoner.rdfs.RDFSSpecialPropsMapper;
import cn.edu.neu.mitt.mrj.reasoner.rdfs.RDFSSubPropDomRangeMapper;
import cn.edu.neu.mitt.mrj.reasoner.rdfs.RDFSSubPropInheritMapper;
import cn.edu.neu.mitt.mrj.reasoner.rdfs.RDFSSubclasMapper;
import cn.edu.neu.mitt.mrj.reasoner.rdfs.SwapTriplesMapper;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.reasoner.rdfs.DelDuplicatesReducer;
import cn.edu.neu.mitt.mrj.reasoner.rdfs.RDFSSpecialPropsReducer;
import cn.edu.neu.mitt.mrj.reasoner.rdfs.RDFSSubclasReducer;
import cn.edu.neu.mitt.mrj.reasoner.rdfs.RDFSSubpropDomRangeReducer;
import cn.edu.neu.mitt.mrj.reasoner.rdfs.RDFSSubpropInheritReducer;
import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.data.TripleSource;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;

public class RDFSReasoner extends Configured implements Tool {
	
	protected static Logger log = LoggerFactory.getLogger(RDFSReasoner.class);
	private int numMapTasks = -1;
	private int numReduceTasks = -1;
	public static int step = 0;
	private int lastExecutionPropInheritance = -1;
	private int lastExecutionDomRange = -1;

	private void parseArgs(String[] args) {
		
		for(int i=0;i<args.length; ++i) {
			if (args[i].equalsIgnoreCase("--maptasks")) {
				numMapTasks = Integer.valueOf(args[++i]);
			}
			
			if (args[i].equalsIgnoreCase("--reducetasks")) {
				numReduceTasks = Integer.valueOf(args[++i]);
			}
			
			if (args[i].equalsIgnoreCase("--startingStep")) {
				RDFSReasoner.step = Integer.valueOf(args[++i]);
			}

			if (args[i].equalsIgnoreCase("--lastStepPropInher")) {
				lastExecutionPropInheritance = Integer.valueOf(args[++i]);
			}
			
			if (args[i].equalsIgnoreCase("--lastStepDomRange")) {
				lastExecutionDomRange = Integer.valueOf(args[++i]);
			}
		}
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("USAGE: RFDSReasoner [pool path] [options]");
			return;
		}
		
		try {
			ToolRunner.run(new Configuration(), new RDFSReasoner(), args);
		} catch (Exception e) {}
	}
	
	
	// Input from CassandraDB.COLUMNFAMILY_JUSTIFICATIONS
	private void configureInputJob(Job job) {
		//Set the input
        ConfigHelper.setInputInitialAddress(job.getConfiguration(), "localhost");
        ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
        ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), CassandraDB.KEYSPACE, CassandraDB.COLUMNFAMILY_JUSTIFICATIONS);
        CqlConfigHelper.setInputCql(job.getConfiguration(), 
        		"SELECT * FROM " + CassandraDB.KEYSPACE + "." + CassandraDB.COLUMNFAMILY_JUSTIFICATIONS + 
        		" WHERE TOKEN(" + 
        		CassandraDB.COLUMN_SUB + ", " + 
        		CassandraDB.COLUMN_PRE + ", " + 
        		CassandraDB.COLUMN_OBJ + ", " + 
        		CassandraDB.COLUMN_IS_LITERAL + ", " + 
        		CassandraDB.COLUMN_TRIPLE_TYPE + 
        		") > ? AND TOKEN(" + 
        		CassandraDB.COLUMN_SUB + ", " + 
        		CassandraDB.COLUMN_PRE + ", " + 
				CassandraDB.COLUMN_OBJ + ", " + 
        		CassandraDB.COLUMN_IS_LITERAL + ", " + 
				CassandraDB.COLUMN_TRIPLE_TYPE + 
        		") <= ? ALLOW FILTERING");
        CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "3");
        job.setInputFormatClass(CqlInputFormat.class);
	}
	
	
	// Output to CassandraDB.COLUMNFAMILY_JUSTIFICATIONS
	private void configureOutputJob(Job job) {
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
	private Job createNewJob(String jobName, String filenameFilter)
		throws IOException {
		Configuration conf = new Configuration();
		conf.setInt("maptasks", numMapTasks);
		conf.set("input.filter", filenameFilter);
	    
		Job job = new Job(conf);
		job.setJobName(jobName);
		job.setJarByClass(RDFSReasoner.class);
	    job.setNumReduceTasks(numReduceTasks);
	    
	    configureInputJob(job);
	    configureOutputJob(job);
	    
	    // Added by WuGang 2010-05-25
	    System.out.println("创建了任务-" + jobName);
	    
	    return job;
	}

	
	// The derivation will be launched in run()
	public long launchDerivation(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		long time = System.currentTimeMillis();
		
		parseArgs(args);
		Job job = null;
		long derivation = 0;

		// RDFS subproperty inheritance reasoning
		job = createNewJob("RDFS subproperty inheritance reasoning", "FILTER_ONLY_HIDDEN");
		job.setMapperClass(RDFSSubPropInheritMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(RDFSSubpropInheritReducer.class);
		job.getConfiguration().setInt("reasoner.step", ++step);
		job.getConfiguration().setInt("lastExecution.step", lastExecutionPropInheritance);
		lastExecutionPropInheritance = step;
//TODO:		configureOutputJob(job, args[0], "dir-rdfs-derivation/dir-subprop-inherit");
		job.waitForCompletion(true);
		long propInheritanceDerivation = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();

		
		// RDFS subproperty domain and range reasoning
		job = createNewJob("RDFS subproperty domain and range reasoning", "FILTER_ONLY_HIDDEN");
		job.setMapperClass(RDFSSubPropDomRangeMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);	// Modified by WuGang, 2010-08-26
		job.setMapOutputValueClass(LongWritable.class);
		job.setPartitionerClass(MyHashPartitioner.class);	// Is this ok?
		job.setReducerClass(RDFSSubpropDomRangeReducer.class);
		job.getConfiguration().setInt("reasoner.step", ++step);
		job.getConfiguration().setInt("lastExecution.step", lastExecutionDomRange);
		lastExecutionDomRange = step;
//TODO:		configureOutputJob(job, args[0], "dir-rdfs-derivation/dir-subprop-domain-range");
		job.waitForCompletion(true);
		long domRangeDerivation = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();

		
		// RDFS cleaning up subprop duplicates
		// 这里估计是有问题啊，需要看一下Mapper和Reducer的输出是否是文件系统！！！！！！！！！！！！！！！！！！！！！！！！！
		if (propInheritanceDerivation > 0 || domRangeDerivation > 0) {
			job = createNewJob("RDFS cleaning up subprop duplicates", "FILTER_ONLY_HIDDEN");
			job.setMapperClass(SwapTriplesMapper.class);
			job.setMapOutputKeyClass(Triple.class);
			job.setMapOutputValueClass(TripleSource.class);
			job.setReducerClass(DelDuplicatesReducer.class);
			job.getConfiguration().setInt("reasoner.filterStep", step - 2);
//TODO:			configureOutputJob(job, args[0], "dir-rdfs-output/dir-subprop-filtered-" + step);
		    job.waitForCompletion(true);
		    derivation = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
		} else {
			derivation = 0;
		}
	    FileSystem.get(job.getConfiguration()).delete(new Path(args[0],"dir-rdfs-derivation/"), true);

	    //RDFS subclass reasoning
		job = createNewJob("RDFS subclass reasoning", "FILTER_ONLY_TYPE_SUBCLASS");
		job.setMapperClass(RDFSSubclasMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(RDFSSubclasReducer.class);
		job.getConfiguration().setInt("reasoner.step", ++step);
//		configureOutputJob(job, args[0], "dir-rdfs-output/dir-subclass-" + step);
		job.waitForCompletion(true);
		derivation += job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
		
	    //calculate whether I derive some special properties
	    long specialTriples = 0;
	    Counter counter = job.getCounters().findCounter("RDFS derived triples", "subclass of resource");
	    specialTriples += counter.getValue();
	    counter = job.getCounters().findCounter("RDFS derived triples", "subclass of Literal");
	    specialTriples += counter.getValue();
	    counter = job.getCounters().findCounter("RDFS derived triples", "subproperty of member");
	    specialTriples += counter.getValue();
	    
		//Apply special derivations
		if (specialTriples > 0) {
		    job = createNewJob("RDFS special properties reasoning", "FILTER_ONLY_OTHER_SUBCLASS_SUBPROP");
			job.setMapperClass(RDFSSpecialPropsMapper.class);
			job.setMapOutputKeyClass(BytesWritable.class);
			job.setMapOutputValueClass(LongWritable.class);
			job.setReducerClass(RDFSSpecialPropsReducer.class);
			job.getConfiguration().setInt("reasoner.step", ++step);

//			configureOutputJob(job, args[0], "dir-rdfs-output/dir-special-props-" + step);
		    job.waitForCompletion(true);
		    derivation += job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter","REDUCE_OUTPUT_RECORDS").getValue();
		}
		
		log.info("RDFS reasoning time: " + (System.currentTimeMillis() - time));
		log.info("RDFS derivation: " + derivation);
		return derivation;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		launchDerivation(args);
		return 0;
	}
}