/**
 * Project Name: mrj-0.1
 * File Name: DB.java
 * @author Gang Wu
 * 2014Äê10ÔÂ11ÈÕ ÏÂÎç2:39:42
 * 
 * Description: 
 * TODO
 */
package cn.edu.neu.mitt.mrj.io.dbs;


import java.awt.print.Printable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.cli.CliParser.rowKey_return;
import org.apache.cassandra.cql3.statements.MultiColumnRestriction.EQ;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.hsqldb.ResultBase.ResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.data.TripleSource;
import cn.edu.neu.mitt.mrj.justification.OWLHorstJustification;
import cn.edu.neu.mitt.mrj.reasoner.Experiments;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
//modified  cassandra java 2.0.5
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;
//modified 


/**
 * @author gibeo_000
 *
 */
public class CassandraDB {
    private static final Logger logger = LoggerFactory.getLogger(CassandraDB.class);
    public static final String KEYSPACE = "mrjks";	// mr.j keyspace
    public static final String COLUMNFAMILY_JUSTIFICATIONS = "justifications";	// mr.j keyspace
    public static final String COLUMNFAMILY_RESOURCES = "resources";	// mr.j keyspace
    public static final String COLUMNFAMILY_RESULTS = "results";	// mr.j keyspace 
//    public static final String COLUMNFAMILY_ALLTRIPLES = "alltriples";      
    public static final String COLUMN_SUB = "sub";	// mrjks.justifications.sub
    public static final String COLUMN_PRE = "pre";	// mrjks.justifications.pre
    public static final String COLUMN_OBJ = "obj";	// mrjks.justifications.obj
    public static final String  COLUMN_TRIPLE_TYPE = "tripletype" ;	// mrjks.justifications.tripletype
    public static final String COLUMN_IS_LITERAL = "isliteral" ;	// mrjks.justifications.isliteral
    public static final String COLUMN_INFERRED_STEPS = "inferredsteps" ;	// mrjks.justifications.inferredsteps    
    public static final String COLUMN_RULE = "rule"; // mrjks.justifications.rule
    public static final String COLUMN_V1 = "v1"; // mrjks.justifications.rule
    public static final String COLUMN_V2 = "v2"; // mrjks.justifications.rule
    public static final String COLUMN_V3 = "v3"; // mrjks.justifications.rule
    public static final String COLUMN_ID = "id";	// mrjks.resources.id
    public static final String COLUMN_LABEL = "label";	// mrjks.resources.label
    public static final String COLUMN_JUSTIFICATION = "justification";	//mrjks.results.justification
    public static final String COLUMN_TRANSITIVE_LEVELS = "transitivelevel";	// mrjks.results.step
    
    public static final String DEFAULT_HOST = cn.edu.neu.mitt.mrj.utils.Cassandraconf.host;
    public static final String DEFAULT_PORT = "9160";	// in version 2.1.2, cql3 port is 9042
    
	public static final String CQL_PAGE_ROW_SIZE = "10000";	//3  modified by liyang

	// Added by WuGang 20160203
	public static Set<Long> domainSchemaTriples = null;
	public static Set<Long> rangeSchemaTriples = null;
	public static Set<Long> memberProperties = null;
	public static Set<Long> resourceSubclasses = null;
	public static Set<Long> literalSubclasses = null;
	public static Set<Long> schemaFunctionalProperties = null;
	public static Set<Long> schemaInverseFunctionalProperties = null;
	public static Set<Long> schemaSymmetricProperties = null;
	public static Set<Long> schemaInverseOfProperties = null;
	public static Set<Long> schemaTransitiveProperties = null;
	public static Set<Long> subclassSchemaTriples = null;
	public static Set<Long> subpropSchemaTriples = null;	
	public static Set<Long> hasValue = null;
	public static Set<Long> hasValueInverted = null;
	public static Set<Long> onProperty = null;
	public static Set<Long> onPropertyInverted = null;
	
	public static Map<Long, Collection<Long>> subclassSchemaTriplesMap = null;
	public static Map<Long, Collection<Long>> domainSchemaTriplesMap = null;
	public static Map<Long, Collection<Long>> rangeSchemaTriplesMap = null;
	public static  Map<Long, Collection<Long>> subpropSchemaTriplesMap = null;

	// 2014-12-11, Very strange, this works around.
    public static final String CONFIG_LOCATION = cn.edu.neu.mitt.mrj.utils.Cassandraconf.CassandraConfFile;
    public static void setConfigLocation(){
    	setConfigLocation(CONFIG_LOCATION);
    }
    public static void setConfigLocation(String location){
    	System.setProperty("cassandra.config", location);
    }

	private Cassandra.Iface client;

    private static Cassandra.Iface createConnection() throws TTransportException{
        if (System.getProperty("cassandra.host") == null || System.getProperty("cassandra.port") == null){
            logger.warn("cassandra.host or cassandra.port is not defined, using default");
        }
        System.out.println("Port : " + System.getProperty("cassandra.port", DEFAULT_PORT));
        return createConnection(System.getProperty("cassandra.host", DEFAULT_HOST),
                                Integer.valueOf(System.getProperty("cassandra.port", DEFAULT_PORT)));
    }

    
    
    private static TSocket socket = null;
    private static TTransport trans = null;
    private static Cassandra.Client c1 = null;
    private static Cassandra.Client createConnection(String host, Integer port) throws TTransportException {
		if (c1 != null) {
			return c1;
		}
		socket = new TSocket(host, port);
		trans = new TFramedTransport(socket);
        trans.open();
        TProtocol protocol = new TBinaryProtocol(trans);
         
        c1 = new Cassandra.Client(protocol);        
        //Modified 2015/5/25
        return c1;
    }
    
    private static void close(){
    	if(trans != null)
    		trans.close();
    	if(socket != null)
    		socket.close();
    	return;
    }
    
    
    private static void setupKeyspace(Cassandra.Iface client)  
            throws InvalidRequestException, 
            UnavailableException, 
            TimedOutException, 
            SchemaDisagreementException, 
            TException {
    	
        KsDef ks;
        try {
            ks = client.describe_keyspace(KEYSPACE);
        } catch(NotFoundException e){
            logger.info("set up keyspace " + KEYSPACE);
            String query = "CREATE KEYSPACE " + KEYSPACE +
                              " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2}"; 

            client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ANY);

            String verifyQuery = "select count(*) from system.peers";
            CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes(verifyQuery), Compression.NONE, ConsistencyLevel.ANY);

            long magnitude = ByteBufferUtil.toLong(result.rows.get(0).columns.get(0).value);
            try {
                Thread.sleep(1000 * magnitude);
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }
    }

    public static String getJustificationsSchema(){
        String schemaString = "CREATE TABLE " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS + 
                " ( " + 
                COLUMN_SUB + " bigint, " +			// partition key
                COLUMN_PRE + " bigint, " +			// partition key
                COLUMN_OBJ + " bigint, " +			// partition key
                COLUMN_IS_LITERAL + " boolean, " +	// partition key
                COLUMN_TRIPLE_TYPE + " int, " +
                COLUMN_RULE + " int, " +
                COLUMN_V1 + " bigint, " +
                COLUMN_V2 + " bigint, " +
                COLUMN_V3 + " bigint, " +
                COLUMN_INFERRED_STEPS + " int, " +	
                COLUMN_TRANSITIVE_LEVELS + " int, " +
                "   PRIMARY KEY ((" + COLUMN_IS_LITERAL + ", " + COLUMN_RULE + ", " + COLUMN_SUB + "), " + COLUMN_TRIPLE_TYPE +
               ", " + COLUMN_PRE + ", " + COLUMN_OBJ + ", " + COLUMN_V1 + ", " + COLUMN_V2 + ", " +  COLUMN_V3 +  
                //", " + COLUMN_TRIPLE_TYPE +
                " ) ) ";
    	return schemaString;
    }
    
    /*
     * ？？
     */
    public static String getJustificationseStatement(){
    	return ("INSERT INTO " + CassandraDB.KEYSPACE + "."  + CassandraDB.COLUMNFAMILY_JUSTIFICATIONS +
    			" (isliteral, rule, sub, tripletype, pre, obj, v1, v2, v3, inferredsteps, transitivelevel) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )");
    }
    
    
//    public static String getAlltripleSchema(){
//    	String ALLTRIPLE_SCHEMA = "CREATE TABLE " + KEYSPACE + "."  + CassandraDB.COLUMNFAMILY_ALLTRIPLES + 
//                " ( " + 
//                COLUMN_SUB + " bigint, " +			// partition key
//                COLUMN_PRE + " bigint, " +			// partition key
//                COLUMN_OBJ + " bigint, " +			// partition key
//                COLUMN_IS_LITERAL + " boolean, " +	// partition key
//                COLUMN_TRIPLE_TYPE + " int, " +
//                COLUMN_INFERRED_STEPS + " int, " +
//                "PRIMARY KEY ((" + COLUMN_SUB + ", " + COLUMN_PRE + "," +  COLUMN_OBJ + 
//                ")) ) WITH compaction = {'class': 'LeveledCompactionStrategy'}";
//    	return ALLTRIPLE_SCHEMA;
//    }
    
    /*
    public static String getStepsSchema(Integer step){
    	String 	STEPS_SCHEMA = "CREATE TABLE " + CassandraDB.KEYSPACE + ".step"  + step + 
				" ( " + 
				COLUMN_SUB + " bigint, " + 
				COLUMN_PRE + " bigint, " +
				COLUMN_OBJ + " bigint, " +	
        		COLUMN_RULE + " int, " +
                COLUMN_V1 + " bigint, " +
                COLUMN_V2 + " bigint, " +
                COLUMN_V3 + " bigint, " +
                COLUMN_TRANSITIVE_LEVELS + " int, " + 
                "PRIMARY KEY((" + COLUMN_SUB + ", " + COLUMN_PRE + ", " + COLUMN_OBJ + ", " + COLUMN_RULE + 
                "), " + COLUMN_V1 + ", " + COLUMN_V2 + ", " + COLUMN_V3 +
                ")) WITH compaction = {'class': 'LeveledCompactionStrategy'}"; 
    	return STEPS_SCHEMA;
    }
    
    public static String getStepsSchema(String cfName){
    	String 	STEPS_SCHEMA = "CREATE TABLE " + CassandraDB.KEYSPACE + "." + cfName + 
				" ( " + 
				COLUMN_SUB + " bigint, " + 
				COLUMN_PRE + " bigint, " +
				COLUMN_OBJ + " bigint, " +	
        		COLUMN_RULE + " int, " +
                COLUMN_V1 + " bigint, " +
                COLUMN_V2 + " bigint, " +
                COLUMN_V3 + " bigint, " +
                COLUMN_TRANSITIVE_LEVELS + " int, " + 
                "PRIMARY KEY((" + COLUMN_SUB + ", " + COLUMN_PRE + ", " + COLUMN_OBJ + ", " + COLUMN_RULE + 
                "), " + COLUMN_V1 + ", " + COLUMN_V2 + ", " + COLUMN_V3 +
                ")) WITH compaction = {'class': 'LeveledCompactionStrategy'}"; 
    	return STEPS_SCHEMA;
    }
    
    public static String getStepsStatement(int step){
    	String query = "INSERT INTO " + CassandraDB.KEYSPACE + ".step" + step +
    			" (sub, pre, obj, rule, v1, v2, v3, transitivelevel) VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
    	return query;
    }
    
    public static String getStepsStatement(String cfName){
    	String query = "INSERT INTO " + CassandraDB.KEYSPACE + "." + cfName +
    			" (sub, pre, obj, rule, v1, v2, v3, transitivelevel) VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
    	return query;
    }
    
    public static String getAlltripleStatement(){
    	return ("INSERT INTO " + CassandraDB.KEYSPACE + "."  + CassandraDB.COLUMNFAMILY_ALLTRIPLES +
    			" (sub, pre, obj, isliteral, tripletype, inferredsteps) VALUES(?, ?, ?, ?, ?, ?)");
    }
    */
    
    private static void setupTables(Cassandra.Iface client)  
            throws InvalidRequestException, 
            UnavailableException, 
            TimedOutException, 
            SchemaDisagreementException, 
            TException {
 
    	
    	// Create justifications table
        String query = "CREATE TABLE " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS + 
                " ( " + 
                COLUMN_SUB + " bigint, " +			// partition key
                COLUMN_PRE + " bigint, " +			// partition key
                COLUMN_OBJ + " bigint, " +			// partition key
                COLUMN_IS_LITERAL + " boolean, " +	// partition key
                COLUMN_TRIPLE_TYPE + " int, " +
                COLUMN_RULE + " int, " +
                COLUMN_V1 + " bigint, " +
                COLUMN_V2 + " bigint, " +
                COLUMN_V3 + " bigint, " +
//                COLUMN_TRIPLE_TYPE + " int, " +
                COLUMN_INFERRED_STEPS + " int, " +		// from this line, the fields are non-primary key
                COLUMN_TRANSITIVE_LEVELS + " int, " +
                "   PRIMARY KEY ((" + COLUMN_IS_LITERAL + ", " + COLUMN_RULE + ", " + COLUMN_SUB + "), " + COLUMN_TRIPLE_TYPE +
               ", " + COLUMN_PRE + ", " + COLUMN_OBJ + ", " + COLUMN_V1 + ", " + COLUMN_V2 + ", " +  COLUMN_V3 +  
                //", " + COLUMN_TRIPLE_TYPE +
                " ) ) ";

        try {
            logger.info("set up table " + COLUMNFAMILY_JUSTIFICATIONS);
            client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
        } catch (InvalidRequestException e) {
            logger.error("failed to create table " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS, e);
        }

		
        // Create resources table
        query = "CREATE TABLE " + KEYSPACE + "."  + COLUMNFAMILY_RESOURCES + 
                " ( " +
        		COLUMN_ID + " bigint, " +
        		COLUMN_LABEL + " text, " +
                "   PRIMARY KEY (" + COLUMN_ID + ") ) ";

        try {
            logger.info("set up table " + COLUMNFAMILY_RESOURCES);
            client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (InvalidRequestException e) {
            logger.error("failed to create table " + KEYSPACE + "."  + COLUMNFAMILY_RESOURCES, e);
        }
        
       
        // Create results table
        query = "CREATE TABLE " + KEYSPACE + "."  + COLUMNFAMILY_RESULTS + 
                " ( " +
        		"id" + " int, " +
                COLUMN_JUSTIFICATION + " set<frozen <tuple<bigint, bigint, bigint>>>, " +
                "   PRIMARY KEY (" + "id" + ") ) ";
        
        try {
            logger.info("set up table " + COLUMNFAMILY_RESULTS);
            client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (InvalidRequestException e) {
            logger.error("failed to create table " + KEYSPACE + "."  + COLUMNFAMILY_RESULTS, e);
        }
        
        
        //Create resultrow table
        String cquery = "CREATE TABLE IF NOT EXISTS " + CassandraDB.KEYSPACE + "."  + "resultrows" + 
                " ( " + 
                CassandraDB.COLUMN_IS_LITERAL + " boolean, " +	// partition key 
                CassandraDB.COLUMN_RULE + " int, " +
                CassandraDB.COLUMN_SUB + " bigint, " +			// partition key
                CassandraDB.COLUMN_TRIPLE_TYPE + " int, " +
                CassandraDB.COLUMN_PRE + " bigint, " +			// partition key
                CassandraDB.COLUMN_OBJ + " bigint, " +			// partition key
                CassandraDB.COLUMN_V1 + " bigint, " +
                CassandraDB.COLUMN_V2 + " bigint, " +
                CassandraDB.COLUMN_V3 + " bigint, " +
//                COLUMN_TRIPLE_TYPE + " int, " +
				CassandraDB.COLUMN_INFERRED_STEPS + " int, " +		// this is the only field that is not included in the primary key
				CassandraDB.COLUMN_TRANSITIVE_LEVELS + " int, " +
                "   PRIMARY KEY ((" + CassandraDB.COLUMN_IS_LITERAL + ", " + CassandraDB.COLUMN_RULE + ", " + CassandraDB.COLUMN_SUB  + "), " +
                CassandraDB.COLUMN_TRIPLE_TYPE + ", " + CassandraDB.COLUMN_PRE + ", " + CassandraDB.COLUMN_OBJ + ", " + CassandraDB.COLUMN_V1 + ", " + CassandraDB.COLUMN_V2 + ", " +  CassandraDB.COLUMN_V3 +
                //", " + COLUMN_TRIPLE_TYPE +
                " ) ) ";
        client.execute_cql3_query(ByteBufferUtil.bytes(cquery), Compression.NONE, ConsistencyLevel.ONE);	

        /*
         * 建立索引可能失败
         */
        
//		String indexQuery = "CREATE INDEX on resultrows (sub) ;";
//		CqlPreparedResult indexPreparedResult = client.prepare_cql3_query(ByteBufferUtil.bytes(indexQuery), Compression.NONE);
//		indexQuery = "CREATE INDEX on resultrows (obj) ;";
//		indexPreparedResult = client.prepare_cql3_query(ByteBufferUtil.bytes(indexQuery), Compression.NONE);
//		indexQuery = "CREATE INDEX on resultrows (pre) ;";
//		indexPreparedResult = client.prepare_cql3_query(ByteBufferUtil.bytes(indexQuery), Compression.NONE);
//		indexQuery = "CREATE INDEX on resultrows (isliteral) ;";
//		indexPreparedResult = client.prepare_cql3_query(ByteBufferUtil.bytes(indexQuery), Compression.NONE);

		
        /*
    	//创建所有三元组的表
        cquery = "CREATE TABLE " + KEYSPACE + "."  + CassandraDB.COLUMNFAMILY_ALLTRIPLES + 
                " ( " + 
                COLUMN_SUB + " bigint, " +			// partition key
                COLUMN_PRE + " bigint, " +			// partition key
                COLUMN_OBJ + " bigint, " +			// partition key
                COLUMN_IS_LITERAL + " boolean, " +	// partition key
                COLUMN_TRIPLE_TYPE + " int, " +
                COLUMN_INFERRED_STEPS + " int, " +
                "PRIMARY KEY ((" + COLUMN_SUB + ", " + COLUMN_PRE + "," +  COLUMN_OBJ + 
                ")) ) WITH compaction = {'class': 'LeveledCompactionStrategy'}";

        try {
            logger.info("set up table " + "all triples");
            client.execute_cql3_query(ByteBufferUtil.bytes(cquery), Compression.NONE, ConsistencyLevel.ONE);
        } catch (InvalidRequestException e) {
            logger.error("failed to create table " + KEYSPACE + "."  + "AllTriples", e);
        }

    	
		query = "CREATE INDEX ON " + KEYSPACE + "."  + COLUMNFAMILY_ALLTRIPLES + "(" + COLUMN_TRIPLE_TYPE + ")";
		client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE); 	
         */
    }
    
    
    
	public CassandraDB() throws TTransportException {
		client = createConnection();
	}
	
	
	public void CassandraDBClose(){
		this.close();
	}
	
	public void init() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException{
	        setupKeyspace(client);
	        client.set_keyspace(KEYSPACE);
	        setupTables(client);
	        
	        createIndexOnTripleType();
	        createIndexOnresultrows();
	        
	}
	
	public Cassandra.Iface getDBClient(){
		return client;
	}
	
	
	/**
	 * Get the row count according to the COLUMN_INFERRED_STEPS.
	 * @return row count.
	 */
	
	/*
	 *  Need to change
	 */
	
	public long getRowCountAccordingInferredSteps(int level){
		//ALLOW FILTERING
		String query = "SELECT COUNT(*) FROM " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS + 
				" WHERE " + COLUMN_INFERRED_STEPS + " = " + level + " ALLOW FILTERING";

		long num = 0;
		try {
			CqlResult cqlresult = client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
			num = ByteBufferUtil.toLong(cqlresult.getRows().get(0).getColumns().get(0).value);
		} catch (InvalidRequestException e) {
			e.printStackTrace();
		} catch (UnavailableException e) {
			e.printStackTrace();
		} catch (TimedOutException e) {
			e.printStackTrace();
		} catch (SchemaDisagreementException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
		
		return num;
	}
	
	
	//TriplesUtils.SYNONYMS_TABLE
	//TriplesUtils.TRANSITIVE_TRIPLE
	//TriplesUtils.DATA_TRIPLE_SAME_AS
	/**
	 * Get the row count according to the triple type.
	 * @return row count.
	 */
	public long getRowCountAccordingTripleType(int tripletype){
		//ALLOW FILTERING
			
		String query = "SELECT COUNT(*) FROM " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS + 
				" WHERE " + COLUMN_TRIPLE_TYPE + " = " + tripletype + " ALLOW FILTERING";
//		System.out.println(query);
		long num = 0;
		try {
			CqlResult cqlresult = client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
			num = ByteBufferUtil.toLong(cqlresult.getRows().get(0).getColumns().get(0).value);
		} catch (InvalidRequestException e) {
			e.printStackTrace();
		} catch (UnavailableException e) {
			e.printStackTrace();
		} catch (TimedOutException e) {
			e.printStackTrace();
		} catch (SchemaDisagreementException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
		
		return num;
	}
	
	
	/**
	 * Get the row count according to the triple type.
	 * @return row count.
	 */
	public long getRowCountAccordingTripleTypeWithLimitation(int tripletype, int limit){
		//ALLOW FILTERING
		String query = "";
		if (limit <= 0)
			query = "SELECT COUNT(*) FROM " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS + 
					" WHERE " + COLUMN_TRIPLE_TYPE + " = " + tripletype + " ALLOW FILTERING";
		else
			query = "SELECT COUNT(*) FROM " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS + 
					" WHERE " + COLUMN_TRIPLE_TYPE + " = " + tripletype + " LIMIT " + limit + " ALLOW FILTERING ";

		long num = 0;
		try {
			CqlResult cqlresult = client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
			num = ByteBufferUtil.toLong(cqlresult.getRows().get(0).getColumns().get(0).value);
		} catch (InvalidRequestException e) {
			e.printStackTrace();
		} catch (UnavailableException e) {
			e.printStackTrace();
		} catch (TimedOutException e) {
			e.printStackTrace();
		} catch (SchemaDisagreementException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
		
		return num;
	}


	/**
	 * Get the row count according to the type of rule.
	 * @return row count.
	 */
	//modified
	/*
	public long getRowCountAccordingRule(int rule){
		String query = "SELECT COUNT(*) FROM " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS + 
				" WHERE " + COLUMN_RULE + " = " + rule + " ALLOW FILTERING";	// must use ALLOW FILTERING 
		//modified

		long num = 0;
		try {
			CqlResult cqlresult = client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
			num = ByteBufferUtil.toLong(cqlresult.getRows().get(0).getColumns().get(0).value);
		} catch (InvalidRequestException e) {
			e.printStackTrace();
		} catch (UnavailableException e) {
			e.printStackTrace();
		} catch (TimedOutException e) {
			e.printStackTrace();
		} catch (SchemaDisagreementException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
		
		return num;
	}
*/
	
	public void insertResources(long id, String label) throws InvalidRequestException, TException{
        String query = "INSERT INTO " + COLUMNFAMILY_RESOURCES +  
                "(" + COLUMN_ID + ", " + COLUMN_LABEL + ") " +
                " values (?, ?) ";
        List<ByteBuffer> args = new ArrayList<ByteBuffer>();
        args.add(ByteBufferUtil.bytes(id));
        args.add(ByteBufferUtil.bytes(label));
        CqlPreparedResult p_result = client.prepare_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE);
        CqlResult result = client.execute_prepared_cql3_query(p_result.itemId, args, ConsistencyLevel.ANY);
        //logger.info("Number of results: " + result.getNum());
	}

	// TODO it's wrong!!!!!!!!!!
	public void insertJustifications(long sub, long pre, long obj, short rule, long var1, long var2, long var3) 
			throws InvalidRequestException, TException{
        String query = "INSERT INTO " + COLUMNFAMILY_JUSTIFICATIONS +  
                "(id, line) " +
                " values (?, ?) ";
        
        CqlPreparedResult result = client.prepare_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE);
        ByteBufferUtil.bytes(rule);
        
//		client.execute_prepared_cql3_query(result.itemId, arg1, ConsistencyLevel.ANY);
	}
	
	public static Triple readJustificationFromMapReduceRow(Row row){
		Triple result = new Triple();
		long sub = row.getLong(CassandraDB.COLUMN_SUB);
		long pre = row.getLong(CassandraDB.COLUMN_PRE);
		long obj = row.getLong(CassandraDB.COLUMN_OBJ);
		boolean isObjectLiteral = row.getBool(CassandraDB.COLUMN_IS_LITERAL);
		long v1 = -1;
		long v2 = -2;
		long v3 = -3;
		int rule = -4;

		result.setObject(obj);
		result.setObjectLiteral(isObjectLiteral);
		result.setPredicate(pre);
		result.setRobject(v3);
		result.setRpredicate(v2);
		result.setRsubject(v1);
		result.setSubject(sub);
		result.setType(rule);
		return result;
	}
	
	public static int readStepFromMapReduceRow(Row row){
		int step = row.getInt(CassandraDB.COLUMN_INFERRED_STEPS);
		return step;
	}
	
	/*
	public static void writeJustificationToMapReduceMultipleOutputsLessObjects(
			Triple triple, 
			TripleSource source, 
			MultipleOutputs output,
			Map<String, ByteBuffer> keys,
			Map<String, ByteBuffer> allkeys,
			List<ByteBuffer> stepsValues,
			List<ByteBuffer> allTValues,
			String stepname) throws IOException, InterruptedException{

        keys.put(CassandraDB.COLUMN_SUB, ByteBufferUtil.bytes(triple.getSubject()));
        keys.put(CassandraDB.COLUMN_PRE, ByteBufferUtil.bytes(triple.getPredicate()));
        keys.put(CassandraDB.COLUMN_OBJ, ByteBufferUtil.bytes(triple.getObject()));
        keys.put(CassandraDB.COLUMN_RULE, ByteBufferUtil.bytes((int)triple.getType()));	// int
        keys.put(CassandraDB.COLUMN_V1, ByteBufferUtil.bytes(triple.getRsubject()));	// long
        keys.put(CassandraDB.COLUMN_V2, ByteBufferUtil.bytes(triple.getRpredicate()));	// long
        keys.put(CassandraDB.COLUMN_V3, ByteBufferUtil.bytes(triple.getRobject()));	// long
        
        allkeys.put(CassandraDB.COLUMN_SUB, ByteBufferUtil.bytes(triple.getSubject()));
        allkeys.put(CassandraDB.COLUMN_PRE, ByteBufferUtil.bytes(triple.getPredicate()));
        allkeys.put(CassandraDB.COLUMN_OBJ, ByteBufferUtil.bytes(triple.getObject()));
	
    	allTValues.add(ByteBufferUtil.bytes(triple.getSubject()));
    	allTValues.add(ByteBufferUtil.bytes(triple.getPredicate()));
    	allTValues.add(ByteBufferUtil.bytes(triple.getObject()));
    	//用数字直接替代。
    	allTValues.add(triple.isObjectLiteral()?ByteBuffer.wrap(new byte[]{1}):ByteBuffer.wrap(new byte[]{0}));
    	allTValues.add(ByteBufferUtil.bytes(
    			TriplesUtils.getTripleType(
    					source, triple.getSubject(), 
    					triple.getPredicate(), 
    					triple.getObject())));
    	allTValues.add(ByteBufferUtil.bytes((int)source.getStep()));
    	
    	stepsValues.add(ByteBufferUtil.bytes(triple.getSubject()));
    	stepsValues.add(ByteBufferUtil.bytes(triple.getPredicate()));
    	stepsValues.add(ByteBufferUtil.bytes(triple.getObject()));
    	stepsValues.add(ByteBufferUtil.bytes((int)triple.getType()));
    	stepsValues.add(ByteBufferUtil.bytes(triple.getRsubject()));
    	stepsValues.add(ByteBufferUtil.bytes(triple.getRpredicate()));
    	stepsValues.add(ByteBufferUtil.bytes(triple.getRobject()));
    	stepsValues.add(ByteBufferUtil.bytes((int)source.getTransitiveLevel()));  	
    	
    	output.write(CassandraDB.COLUMNFAMILY_ALLTRIPLES, null, allTValues);
    	output.write(stepname, null, stepsValues);


    	keys.clear();
    	allkeys.clear();
    	allTValues.clear();
    	stepsValues.clear();
   	
	}
	 	*/

	/*
	public static void writeJustificationToMapReduceMultipleOutputs(
			Triple triple, 
			TripleSource source, 
			MultipleOutputs output,
			String stepname) throws IOException, InterruptedException{
		Map<String, ByteBuffer> keys = 	new LinkedHashMap<String, ByteBuffer>();
		Map<String, ByteBuffer> allkeys = 	new LinkedHashMap<String, ByteBuffer>();
    	List<ByteBuffer> allvariables =  new ArrayList<ByteBuffer>();
//		long time = System.currentTimeMillis();
	
    	byte one = 1;
    	byte zero = 0;
	    // Prepare composite key (sub, pre, obj)
        keys.put(CassandraDB.COLUMN_SUB, ByteBufferUtil.bytes(triple.getSubject()));
        keys.put(CassandraDB.COLUMN_PRE, ByteBufferUtil.bytes(triple.getPredicate()));
        keys.put(CassandraDB.COLUMN_OBJ, ByteBufferUtil.bytes(triple.getObject()));
    	// the length of boolean type in cassandra is one byte!!!!!!!!
        keys.put(CassandraDB.COLUMN_RULE, ByteBufferUtil.bytes((int)triple.getType()));	// int
        keys.put(CassandraDB.COLUMN_V1, ByteBufferUtil.bytes(triple.getRsubject()));	// long
        keys.put(CassandraDB.COLUMN_V2, ByteBufferUtil.bytes(triple.getRpredicate()));	// long
        keys.put(CassandraDB.COLUMN_V3, ByteBufferUtil.bytes(triple.getRobject()));	// long
        
        allkeys.put(CassandraDB.COLUMN_SUB, ByteBufferUtil.bytes(triple.getSubject()));
        allkeys.put(CassandraDB.COLUMN_PRE, ByteBufferUtil.bytes(triple.getPredicate()));
        allkeys.put(CassandraDB.COLUMN_OBJ, ByteBufferUtil.bytes(triple.getObject()));
        
        allvariables.add(ByteBufferUtil.bytes(source.getStep()));
        allvariables.add(triple.isObjectLiteral()?ByteBuffer.wrap(new byte[]{one}):ByteBuffer.wrap(new byte[]{zero}));        
        allvariables.add(ByteBufferUtil.bytes((int)triple.getType()));
        
        // Prepare variables
    	List<ByteBuffer> variables =  new ArrayList<ByteBuffer>();
//      variables.add(ByteBufferUtil.bytes(oValue.getSubject()));
    	// the length of boolean type in cassandra is one byte!!!!!!!!
    	// For column inferred, init it as false i.e. zero
        //variables.add(ByteBuffer.wrap(new byte[]{zero}));

    	variables.add(ByteBufferUtil.bytes(source.getTransitiveLevel()));
    	


    	// Keys are not used for 
    	//   CqlBulkRecordWriter.write(Object key, List<ByteBuffer> values), 
    	//   so it can be set to null.
    	// Only values are used there where the value correspond to 
    	//   the insert statement set in CqlBulkOutputFormat.setColumnFamilyInsertStatement()
    	// All triples columnfamily:
    	//     sub, pre, obj, isliteral, tripletype, inferredsteps
    	// Steps columnfamily:
    	//     sub, pre, obj, rule, v1, v2, v3, transitivelevel
    	
    	List<ByteBuffer> allTValues =  new ArrayList<ByteBuffer>();
    	allTValues.add(ByteBufferUtil.bytes(triple.getSubject()));
    	allTValues.add(ByteBufferUtil.bytes(triple.getPredicate()));
    	allTValues.add(ByteBufferUtil.bytes(triple.getObject()));
    	allTValues.add(triple.isObjectLiteral()?ByteBuffer.wrap(new byte[]{one}):ByteBuffer.wrap(new byte[]{zero}));
    	allTValues.add(ByteBufferUtil.bytes(
    			TriplesUtils.getTripleType(
    					source, triple.getSubject(), 
    					triple.getPredicate(), 
    					triple.getObject())));
    	allTValues.add(ByteBufferUtil.bytes((int)source.getStep()));
    	
    	List<ByteBuffer> stepsValues =  new ArrayList<ByteBuffer>();
    	stepsValues.add(ByteBufferUtil.bytes(triple.getSubject()));
    	stepsValues.add(ByteBufferUtil.bytes(triple.getPredicate()));
    	stepsValues.add(ByteBufferUtil.bytes(triple.getObject()));
    	stepsValues.add(ByteBufferUtil.bytes((int)triple.getType()));
    	stepsValues.add(ByteBufferUtil.bytes(triple.getRsubject()));
    	stepsValues.add(ByteBufferUtil.bytes(triple.getRpredicate()));
    	stepsValues.add(ByteBufferUtil.bytes(triple.getRobject()));
    	stepsValues.add(ByteBufferUtil.bytes((int)source.getTransitiveLevel()));  	
    	
//    	time = System.currentTimeMillis();
    	output.write(CassandraDB.COLUMNFAMILY_ALLTRIPLES, null, allTValues);
//		System.out.println("wrote all " + (System.currentTimeMillis() - time));
//		System.out.println("write all " + (System.currentTimeMillis() - time));//    	_output.write(stepname, keys, variables);
//		time = System.currentTimeMillis();
    	output.write(stepname, null, stepsValues);
//		System.out.println("wrote steps" + (System.currentTimeMillis() - time));

    	
	}
	*/
/*
	public static void writeJustificationToMapReduceContext(
			Triple triple, 
			TripleSource source, 
			Context context,
			String stepname) throws IOException, InterruptedException{
		Map<String, ByteBuffer> keys = 	new LinkedHashMap<String, ByteBuffer>();
		Map<String, ByteBuffer> allkeys = 	new LinkedHashMap<String, ByteBuffer>();
    	List<ByteBuffer> allvariables =  new ArrayList<ByteBuffer>();
		long time = System.currentTimeMillis();

    	byte one = 1;
    	byte zero = 0;

	    // Prepare composite key (sub, pre, obj)
        keys.put(CassandraDB.COLUMN_SUB, ByteBufferUtil.bytes(triple.getSubject()));
        keys.put(CassandraDB.COLUMN_PRE, ByteBufferUtil.bytes(triple.getPredicate()));
        keys.put(CassandraDB.COLUMN_OBJ, ByteBufferUtil.bytes(triple.getObject()));
    	// the length of boolean type in cassandra is one byte!!!!!!!!
        keys.put(CassandraDB.COLUMN_IS_LITERAL, 
        		triple.isObjectLiteral()?ByteBuffer.wrap(new byte[]{one}):ByteBuffer.wrap(new byte[]{zero}));
        int tripletype = TriplesUtils.DATA_TRIPLE;
        if (triple.getType()==TriplesUtils.OWL_HORST_SYNONYMS_TABLE){
        	tripletype = TriplesUtils.SYNONYMS_TABLE;	// In this way, we can provide a special triple type for triples in synonyms table 
        }else{
        	tripletype = TriplesUtils.getTripleType(source, triple.getSubject(), triple.getPredicate(), triple.getObject());
        }
        keys.put(CassandraDB.COLUMN_TRIPLE_TYPE, ByteBufferUtil.bytes(tripletype));	// Modified by WuGang 20150109
        keys.put(CassandraDB.COLUMN_RULE, ByteBufferUtil.bytes((int)triple.getType()));	// int
        keys.put(CassandraDB.COLUMN_V1, ByteBufferUtil.bytes(triple.getRsubject()));	// long
        keys.put(CassandraDB.COLUMN_V2, ByteBufferUtil.bytes(triple.getRpredicate()));	// long
        keys.put(CassandraDB.COLUMN_V3, ByteBufferUtil.bytes(triple.getRobject()));	// long
        
        // Prepare variables
    	List<ByteBuffer> variables =  new ArrayList<ByteBuffer>();
//      variables.add(ByteBufferUtil.bytes(oValue.getSubject()));
    	// the length of boolean type in cassandra is one byte!!!!!!!!
    	// For column inferred, init it as false i.e. zero
//      variables.add(ByteBuffer.wrap(new byte[]{zero}));
    	variables.add(ByteBufferUtil.bytes(source.getStep()));		// It corresponds to COLUMN_INFERRED_STEPS where steps = 0 means an original triple 
    	variables.add(ByteBufferUtil.bytes(source.getTransitiveLevel()));		// It corresponds to COLUMN_TRANSITIVE_LEVEL, only useful in owl's transitive  
        context.write(keys, variables);
	}
*/
	/*
	public static void writealltripleToMapReduceContext(
			Triple triple, 
			TripleSource source, 
			Context context) throws IOException, InterruptedException{
		Map<String, ByteBuffer> keys = 	new LinkedHashMap<String, ByteBuffer>();

    	byte one = 1;
    	byte zero = 0;

	    // Prepare composite key (sub, pre, obj)
        keys.put(CassandraDB.COLUMN_SUB, ByteBufferUtil.bytes(triple.getSubject()));
        keys.put(CassandraDB.COLUMN_PRE, ByteBufferUtil.bytes(triple.getPredicate()));
        keys.put(CassandraDB.COLUMN_OBJ, ByteBufferUtil.bytes(triple.getObject()));
    	// the length of boolean type in cassandra is one byte!!!!!!!!
        keys.put(CassandraDB.COLUMN_IS_LITERAL, 
        		triple.isObjectLiteral()?ByteBuffer.wrap(new byte[]{one}):ByteBuffer.wrap(new byte[]{zero}));
        int tripletype = TriplesUtils.DATA_TRIPLE;
        if (triple.getType()==TriplesUtils.OWL_HORST_SYNONYMS_TABLE){
        	tripletype = TriplesUtils.SYNONYMS_TABLE;	// In this way, we can provide a special triple type for triples in synonyms table 
        }else{
        	tripletype = TriplesUtils.getTripleType(source, triple.getSubject(), triple.getPredicate(), triple.getObject());
        }
        keys.put(CassandraDB.COLUMN_TRIPLE_TYPE, ByteBufferUtil.bytes(tripletype));	// Modified by WuGang 20150109
        keys.put(CassandraDB.COLUMN_RULE, ByteBufferUtil.bytes((int)triple.getType()));	// int
        keys.put(CassandraDB.COLUMN_V1, ByteBufferUtil.bytes(triple.getRsubject()));	// long
        keys.put(CassandraDB.COLUMN_V2, ByteBufferUtil.bytes(triple.getRpredicate()));	// long
        keys.put(CassandraDB.COLUMN_V3, ByteBufferUtil.bytes(triple.getRobject()));	// long
        
        // Prepare variables
    	List<ByteBuffer> variables =  new ArrayList<ByteBuffer>();
//      variables.add(ByteBufferUtil.bytes(oValue.getSubject()));
    	// the length of boolean type in cassandra is one byte!!!!!!!!
    	// For column inferred, init it as false i.e. zero
//      variables.add(ByteBuffer.wrap(new byte[]{zero}));
    	variables.add(ByteBufferUtil.bytes(source.getStep()));		// It corresponds to COLUMN_INFERRED_STEPS where steps = 0 means an original triple 
    	variables.add(ByteBufferUtil.bytes(source.getTransitiveLevel()));		// It corresponds to COLUMN_TRANSITIVE_LEVEL, only useful in owl's transitive  
        context.write(keys, variables);
	}
	*/
	
	public static void writeJustificationToMapReduceContext(
			Triple triple, 
			TripleSource source, 
			Context context) throws IOException, InterruptedException{
		Map<String, ByteBuffer> keys = 	new LinkedHashMap<String, ByteBuffer>();

    	byte one = 1;
    	byte zero = 0;

	    // Prepare composite key (sub, pre, obj)
//        keys.put(CassandraDB.COLUMN_SUB, ByteBufferUtil.bytes(triple.getSubject()));
//        keys.put(CassandraDB.COLUMN_PRE, ByteBufferUtil.bytes(triple.getPredicate()));
//        keys.put(CassandraDB.COLUMN_OBJ, ByteBufferUtil.bytes(triple.getObject()));
//    	// the length of boolean type in cassandra is one byte!!!!!!!!
//        keys.put(CassandraDB.COLUMN_IS_LITERAL, 
//        		triple.isObjectLiteral()?ByteBuffer.wrap(new byte[]{one}):ByteBuffer.wrap(new byte[]{zero}));
//        int tripletype = TriplesUtils.DATA_TRIPLE;
//        if (triple.getType()==TriplesUtils.OWL_HORST_SYNONYMS_TABLE){
//        	tripletype = TriplesUtils.SYNONYMS_TABLE;	// In this way, we can provide a special triple type for triples in synonyms table 
//        }else{
//        	tripletype = TriplesUtils.getTripleType(source, triple.getSubject(), triple.getPredicate(), triple.getObject());
//        }
//        keys.put(CassandraDB.COLUMN_TRIPLE_TYPE, ByteBufferUtil.bytes(tripletype));	// Modified by WuGang 20150109
//        keys.put(CassandraDB.COLUMN_RULE, ByteBufferUtil.bytes((int)triple.getType()));	// int
//        keys.put(CassandraDB.COLUMN_V1, ByteBufferUtil.bytes(triple.getRsubject()));	// long
//        keys.put(CassandraDB.COLUMN_V2, ByteBufferUtil.bytes(triple.getRpredicate()));	// long
//        keys.put(CassandraDB.COLUMN_V3, ByteBufferUtil.bytes(triple.getRobject()));	// long

        // Prepare variables
    	List<ByteBuffer> variables =  new ArrayList<ByteBuffer>();
//      variables.add(ByteBufferUtil.bytes(oValue.getSubject()));
    	// the length of boolean type in cassandra is one byte!!!!!!!!
    	// For column inferred, init it as false i.e. zero
//      variables.add(ByteBuffer.wrap(new byte[]{zero}));
    	
		int tripletype = TriplesUtils.DATA_TRIPLE;
		if (triple.getType() == TriplesUtils.OWL_HORST_SYNONYMS_TABLE) {
			tripletype = TriplesUtils.SYNONYMS_TABLE;
		} else {
			tripletype = TriplesUtils.getTripleType(source,
					triple.getSubject(), triple.getPredicate(),
					triple.getObject());
		}
		

		variables.add(triple.isObjectLiteral() ? ByteBuffer
				.wrap(new byte[] { one }) : ByteBuffer
				.wrap(new byte[] { zero }));
		variables.add(ByteBufferUtil.bytes((int) triple.getType()));
		variables.add(ByteBufferUtil.bytes(triple.getSubject()));
		
		variables.add(ByteBufferUtil.bytes(tripletype));
		variables.add(ByteBufferUtil.bytes(triple.getPredicate()));
		variables.add(ByteBufferUtil.bytes(triple.getObject()));
		
    	variables.add(ByteBufferUtil.bytes(triple.getRsubject()));
		variables.add(ByteBufferUtil.bytes(triple.getRpredicate()));
		variables.add(ByteBufferUtil.bytes(triple.getRobject()));

    	variables.add(ByteBufferUtil.bytes(source.getStep()));		// It corresponds to COLUMN_INFERRED_STEPS where steps = 0 means an original triple 
    	variables.add(ByteBufferUtil.bytes(source.getTransitiveLevel()));
    	context.write(null, variables);
	}
	
	public boolean loadSetIntoMemory(Set<Long> schemaTriples, Set<Integer> filters, int previousStep) throws IOException, InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException {
		return loadSetIntoMemory(schemaTriples, filters, previousStep, false);
	}
	
	public String idToLabel(long id) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException, CharacterCodingException{
		String query = "SELECT " + COLUMN_LABEL + " FROM " + KEYSPACE + "."  + COLUMNFAMILY_RESOURCES + 
				" WHERE " + COLUMN_ID + "=" + id;
		CqlResult cqlResult = client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
		for (CqlRow row : cqlResult.rows){
  			Column column = row.columns.get(0);						// Only one column COLUMN_LABEL
  			return ByteBufferUtil.string(column.bufferForValue());	// Only one row in fact, otherwise return null;
		}
		
		return null;
	}
	
	public static Set<Set<TupleValue>> getJustifications() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException, IOException, ClassNotFoundException, RequestExecutionException{
		Set<Set<TupleValue>> results = new HashSet<Set<TupleValue>>();
		
//		String query = "SELECT " + COLUMN_JUSTIFICATION + " FROM " + KEYSPACE + "."  + COLUMNFAMILY_RESULTS;
		SimpleClientDataStax scds = new SimpleClientDataStax();
		scds.connect(DEFAULT_HOST);
		
		//Modified 2015-6-25
		//From  COLUMNFAMILY_RESULTS to justifications	??\\
		Statement statement = QueryBuilder.select().all().from(KEYSPACE, "results").where(QueryBuilder.eq("id", OWLHorstJustification.id));
		List<Row> rows = scds.getSession().execute(statement).all();

		for (Row row : rows){
			//modified
			Set<TupleValue> testResult = row.getSet("justification", TupleValue.class);
        	Set<Set<TupleValue>> toBeDeletedFromResults = new HashSet<Set<TupleValue>>();	// Perform delete these from the results
        	boolean beAdded = true;
		        for (Set<TupleValue> currentResult : results){
		        	if (testResult.containsAll(currentResult)){
		        		beAdded = false;
		        		break;	// testResult contains (is larger than) currentResult, so testResult should not be added
		        	}
		        	else if (currentResult.containsAll(testResult)){
		        		// currentResult contains (is larger than) testResult, so currentResult should be deleted from the results set after traversing the result set
		        		toBeDeletedFromResults.add(currentResult);	
		        	}
		        }
		        if (beAdded)	// The testResul2.5 <http?t is a candidate justification
		        	results.add(testResult);
		}
		
		scds.close();
		

		/*CqlResult cqlResult = client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
		for (CqlRow row : cqlResult.rows){
  			Column column = row.columns.get(0);	// Only one column COLUMN_JUSTIFICATION
  		    if (new String(column.getName()).equals(COLUMN_JUSTIFICATION)){
//  		    	ByteArrayInputStream bi = new ByteArrayInputStream(column.getValue());
//  		        ObjectInputStream oi = new ObjectInputStream(bi);
  		    	InputStream is = ByteBufferUtil.inputStream(column.bufferForValue());
  		        ObjectInputStream oi = new ObjectInputStream(is);
  		        @SuppressWarnings("unchecked")
				Set<TupleValue> testResult = (Set<TupleValue>)oi.readObject();	// A new result from db to be test whether a justification
	        	Set<Set<TupleValue>> toBeDeletedFromResults = new HashSet<Set<TupleValue>>();	// Perform delete these from the results
	        	boolean beAdded = true;
  		        for (Set<TupleValue> currentResult : results){
  		        	if (testResult.containsAll(currentResult)){
  		        		beAdded = false;
  		        		break;	// testResult contains (is larger than) currentResult, so testResult should not be added
  		        	}
  		        	else if (currentResult.containsAll(testResult)){
  		        		// currentResult contains (is larger than) testResult, so currentResult should be deleted from the results set after traversing the result set
  		        		toBeDeletedFromResults.add(currentResult);	
  		        	}
  		        }
  		        if (beAdded)	// The testResult is a candidate justification
  		        	results.add(testResult);
  		    }
		}*/
	//modified  cassandra java 2.0.5
		return results;
	}
	
	
	public Set<Triple> getTracingEntries(Triple triple) throws InvalidRequestException, TException{
    	byte one = 1;
    	byte zero = 0;
		Set<Triple> tracingEntries = new HashSet<Triple>(); 
		
		//Fixed 2016/4/13

		String query = "SELECT * FROM " + KEYSPACE + "."  + "resultrows" + " WHERE " + 
				COLUMN_SUB + "=? AND " + COLUMN_PRE + "=? AND " + COLUMN_OBJ + "=? AND " + COLUMN_IS_LITERAL + "=? ALLOW FILTERING";
		CqlPreparedResult preparedResult = client.prepare_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE);
    	List<ByteBuffer> list = new ArrayList<ByteBuffer>();
    	list.add(ByteBufferUtil.bytes(triple.getSubject()));
    	list.add(ByteBufferUtil.bytes(triple.getPredicate()));
    	list.add(ByteBufferUtil.bytes(triple.getObject()));
    	list.add(triple.isObjectLiteral()?ByteBuffer.wrap(new byte[]{one}):ByteBuffer.wrap(new byte[]{zero}));
    	CqlResult result = client.execute_prepared_cql3_query(preparedResult.itemId, list, ConsistencyLevel.ONE);
       	for(CqlRow row : result.rows){
    		Triple tracingEntry = new Triple(triple);
    		Iterator<Column> columnsIt = row.getColumnsIterator();
      		while (columnsIt.hasNext()) {
      			Column column = columnsIt.next();
      		    if (new String(column.getName()).equals(COLUMN_RULE))
      		    	tracingEntry.setType(ByteBufferUtil.toInt(column.bufferForValue()));
      		    if (new String(column.getName()).equals(COLUMN_V1))
      		    	tracingEntry.setRsubject(ByteBufferUtil.toLong(column.bufferForValue()));
      		    if (new String(column.getName()).equals(COLUMN_V2))
      		    	tracingEntry.setRpredicate(ByteBufferUtil.toLong(column.bufferForValue()));
      		    if (new String(column.getName()).equals(COLUMN_V3))
      		    	tracingEntry.setRobject(ByteBufferUtil.toLong(column.bufferForValue()));
     		}
      		tracingEntries.add(tracingEntry);
       	}
       	
		return tracingEntries;
	}
	
	public boolean loadSetIntoMemory(
			Set<Long> schemaTriples, Set<Integer> filters, 
			int previousStep, boolean inverted)	throws IOException, InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException {
		long startTime = System.currentTimeMillis();
		boolean schemaChanged = false;

		logger.info("In CassandraDB's loadSetIntoMemory");
		
		// Require an index created on COLUMN_TRIPLE_TYPE column
		/*
		 * Be Attention
		 * add ALLOW FILTERING
		 * 2015/6/12
		 */
		

        String query = "SELECT " + COLUMN_SUB + ", " + COLUMN_OBJ +  ", " + COLUMN_INFERRED_STEPS +
        		" FROM " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS +  
                " WHERE " + COLUMN_TRIPLE_TYPE + " = ? ALLOW FILTERING";
        System.out.println(query);
        
        CqlPreparedResult preparedResult = client.prepare_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE);

        for (int filter : filters){
        	List<ByteBuffer> list = new ArrayList<ByteBuffer>();
        	list.add(ByteBufferUtil.bytes(filter));
//        	System.out.println("filter " + filter);
        	CqlResult result = client.execute_prepared_cql3_query(preparedResult.itemId, list, ConsistencyLevel.ONE);
        	Iterator<CqlRow> it =result.getRowsIterator();
        	while(it.hasNext() ){
        		CqlRow row = it.next();
//        	for(CqlRow row : result.rows){
        		Iterator<Column> columnsIt = row.getColumnsIterator();
      			Long sub = null, obj = null;
//      			System.out.println("row : " + row);
	      		while (columnsIt.hasNext()) {
	      			Column column = columnsIt.next();
	      		    if (new String(column.getName()).equals(COLUMN_SUB))
	      		    	sub = ByteBufferUtil.toLong(column.bufferForValue());
	      		    if (new String(column.getName()).equals(COLUMN_OBJ))
	      		    	obj = ByteBufferUtil.toLong(column.bufferForValue());
	      		    if (new String(column.getName()).equals(COLUMN_INFERRED_STEPS)){
	      		    	int step = ByteBufferUtil.toInt(column.bufferForValue());
	      		    	if (step > previousStep)
							schemaChanged = true;
	      		    }
	     		}
    			if (!inverted)
    				schemaTriples.add(sub);  			
        		else
        			schemaTriples.add(obj);
    			
    			System.out.println("schema : " + schemaTriples);

        	}
        }
		
		logger.debug("Time for CassandraDB's loadSetIntoMemory " + (System.currentTimeMillis() - startTime));
		return schemaChanged;
	}
	
	
	public Map<Long, Collection<Long>> loadMapIntoMemory(Set<Integer> filters) throws IOException, InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException {
		return loadMapIntoMemory(filters, false);
	}
	
	// ���ص�key����triple��subject��value��object
	/*
	 * Be Attention
	 * add ALLOW FILTERING
	 * 2015/6/12
	 */
	public Map<Long, Collection<Long>> loadMapIntoMemory(Set<Integer> filters, boolean inverted) throws IOException, InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException {
		long startTime = System.currentTimeMillis();

		logger.info("In CassandraDB's loadMapIntoMemory");

		Map<Long, Collection<Long>> schemaTriples = new HashMap<Long, Collection<Long>>();

		// Require an index created on COLUMN_TRIPLE_TYPE column
        String query = "SELECT " + COLUMN_SUB + ", " + COLUMN_OBJ + ", " + COLUMN_INFERRED_STEPS +
        		" FROM " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS +  
                " WHERE " + COLUMN_TRIPLE_TYPE + " = ? ALLOW FILTERING"; //partitonkey 
        
        CqlPreparedResult preparedResult = client.prepare_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE);

        for (int filter : filters){
        	List<ByteBuffer> list = new ArrayList<ByteBuffer>();
        	list.add(ByteBufferUtil.bytes(filter));
        	CqlResult result = client.execute_prepared_cql3_query(preparedResult.itemId, list, ConsistencyLevel.ONE);
        	for(CqlRow row : result.rows){
        		Iterator<Column> columnsIt = row.getColumnsIterator();
      			Long sub = null, obj = null;
	      		while (columnsIt.hasNext()) {
	      			Column column = columnsIt.next();
	      		    if (new String(column.getName()).equals(COLUMN_SUB))
	      		    	sub = ByteBufferUtil.toLong(column.bufferForValue());
	      		    if (new String(column.getName()).equals(COLUMN_OBJ))
	      		    	obj = ByteBufferUtil.toLong(column.bufferForValue());
	     		}
  
				long tripleKey = 0;
				long tripleValue = 0;
				if (!inverted){
					tripleKey = sub;
					tripleValue = obj;
        		}else{
					tripleKey = obj;
					tripleValue = sub;
        		}
				Collection<Long> cTriples = schemaTriples.get(tripleKey);
				if (cTriples == null) {
					cTriples = new ArrayList<Long>();
					schemaTriples.put(tripleKey, cTriples);
				}
				cTriples.add(tripleValue);						

        	}
        }
		
		logger.info("Time for CassandraDB's loadMapIntoMemory " + (System.currentTimeMillis() - startTime));

		return schemaTriples;
	}	


	// Created index on COLUMN_TRIPLE_TYPE column
	public void createIndexOnTripleType() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException{
		String query = "CREATE INDEX ON " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS + "(" + COLUMN_TRIPLE_TYPE + ")";
		client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
	}
	
	public void createIndexOnInferredSteps() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException{
		String query = "CREATE INDEX ON " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS + "(" + COLUMN_INFERRED_STEPS + ")";
		client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
	}
	
	public void createIndexOnresultrows() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException{
		
		String query = "CREATE INDEX on resultrows (sub) ;";
		client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
		query = "CREATE INDEX on resultrows (obj) ;";
		client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
		query = "CREATE INDEX on resultrows (pre) ;";
		client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
		query = "CREATE INDEX on resultrows (isliteral) ;";
		client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
	
	}

	
//	public void createIndexOnRule() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException{
//		String query = "CREATE INDEX ON " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS + "(" + COLUMN_RULE + ")";
//		client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
//	}
//
//	
//	public void createIndexOnTransitiveLevel() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException{
//		String query = "CREATE INDEX ON " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS + "(" + COLUMN_TRANSITIVE_LEVELS + ")";
//		client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
//	}
	
	/*

	public void Index() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException{
		//createIndexOnInferredSteps();
		createIndexOnRule();
		createIndexOnTransitiveLevel();
		createIndexOnTripleType();
		System.out.println("IndexED");
	}
	
	public void DropTripleTypeIndex() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException{
		String query = "DROP INDEX mrjks.justifications_tripletype_idx";
		client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
	}
	
	public void DropRuleIndex() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException{
		String query = "DROP INDEX mrjks.justifications_rule_idx";
		client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
	}
	
	public void DropInferredStepsIndex() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException{
		String query = "DROP INDEX mrjks.justifications_inferredSteps_idx";
		client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
	}
	
	public void DropTransitiveLevelIndex() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException{
		String query = "DROP INDEX mrjks.justifications_transitiveLevel_idx";
		client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
	}
	
	public void UnIndex() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException{
		
		this.DropInferredStepsIndex();
		this.DropRuleIndex();
		this.DropTransitiveLevelIndex();
		this.DropTripleTypeIndex();
	}
	*/
	// Added by WuGang 2015-06-08
	
	public static ResultSet getRows(){
		Builder builder = Cluster.builder();
		builder.addContactPoint(DEFAULT_HOST);
		SocketOptions socketoptions= new SocketOptions().setKeepAlive(true).setReadTimeoutMillis(10 * 10000).setConnectTimeoutMillis(5 * 10000);
		Cluster clu = builder.build();
		Session session = clu.connect();
		SimpleStatement statement = new SimpleStatement("SELECT sub, obj, pre, isliteral FROM mrjks.justifications where inferredsteps = 0");
		statement.setFetchSize(100);
		ResultSet results = session.execute(statement);
		System.out.println("------------------" + results + "--------------");
		return results;
	}
	
	public static boolean delornot = false;
/*
	public static void removeOriginalTriples(){
		if (delornot == true)
			return;
		delornot = true;
		//ִ�в�Ӧ�жϡ�
		Builder builder = Cluster.builder();
		builder.addContactPoint(DEFAULT_HOST);
		SocketOptions socketoptions= new SocketOptions().setKeepAlive(true).setReadTimeoutMillis(10 * 10000).setConnectTimeoutMillis(5 * 10000);
		Cluster clu = builder.build();
		Session session = clu.connect();
		
        String cquery1 = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "."  + "ruleiszero" + 
                " ( " + 
                COLUMN_SUB + " bigint, " +			// partition key
                COLUMN_PRE + " bigint, " +			// partition key
                COLUMN_OBJ + " bigint, " +			// partition key
                COLUMN_IS_LITERAL + " boolean, " +	// partition key
                COLUMN_TRIPLE_TYPE + " int, " +
                COLUMN_RULE + " int, " +
                COLUMN_V1 + " bigint, " +
                COLUMN_V2 + " bigint, " +
                COLUMN_V3 + " bigint, " +
//                COLUMN_TRIPLE_TYPE + " int, " +
                COLUMN_INFERRED_STEPS + " int, " +		// from this line is non-primary key
                COLUMN_TRANSITIVE_LEVELS + " int, " +	
                "   PRIMARY KEY ((" + COLUMN_SUB + ", " + COLUMN_PRE + ", " + COLUMN_OBJ +  ", " + COLUMN_IS_LITERAL + "), " +
                COLUMN_TRIPLE_TYPE + ", " + COLUMN_RULE + ", " + COLUMN_V1 + ", " + COLUMN_V2 + ", " +  COLUMN_V3 +  
                //", " + COLUMN_TRIPLE_TYPE +
                " ) ) ";
        session.execute(cquery1);

		//SELECT ALL AND DEL ALL
		SimpleStatement statement = new SimpleStatement("SELECT * FROM mrjks.justifications");
		statement.setFetchSize(100);
		ResultSet results = session.execute(statement);
		
		System.out.println("--------DEL ALL----------");
		for (Row row : results){
			
			if(row.getInt(COLUMN_RULE) != 0){
				session.execute("INSERT INTO mrjks.ruleiszero(sub, pre, obj, isliteral, tripletype, rule, v1, v2, v3, inferredsteps)" + 
			"VALUES (" + 
						row.getLong(COLUMN_SUB) + "," + 
			row.getLong(COLUMN_PRE) + "," + 
						row.getLong(COLUMN_OBJ) + "," + 
			row.getBool(COLUMN_IS_LITERAL) + "," + 
						row.getInt(COLUMN_TRIPLE_TYPE) + "," +
			row.getInt(COLUMN_RULE) + "," +  
						row.getLong(COLUMN_V1) + "," + 
			row.getLong(COLUMN_V2) + "," + 
						row.getLong(COLUMN_V3) + "," + 
			row.getInt(COLUMN_INFERRED_STEPS) + ");");
				System.out.println("-------Insert ----------");
				System.out.println(row);
			}
			
			Statement delete = QueryBuilder.delete()
			.from(KEYSPACE, COLUMNFAMILY_JUSTIFICATIONS)
			.where(QueryBuilder.eq(COLUMN_SUB, row.getLong(CassandraDB.COLUMN_SUB)))
			.and(QueryBuilder.eq(COLUMN_PRE, row.getLong(CassandraDB.COLUMN_PRE)))
			.and(QueryBuilder.eq(COLUMN_OBJ, row.getLong(CassandraDB.COLUMN_OBJ)))
			.and(QueryBuilder.eq(COLUMN_IS_LITERAL, row.getBool(COLUMN_IS_LITERAL)));
			session.execute(delete);
			System.out.println(row);
		}
		*/
//		SimpleClientDataStax scds = new SimpleClientDataStax();
//		scds.connect(DEFAULT_HOST);
//		
//		System.out.println("Select Primary Key");
//		//modified select partition key and delete using partition key
//		Statement select = QueryBuilder.select()
//				.all()
//				.from(KEYSPACE, COLUMNFAMILY_JUSTIFICATIONS)
//				.where(QueryBuilder.eq(COLUMN_INFERRED_STEPS, 0));
//		select.setFetchSize(100);
//		ResultSet result = scds.getSession().execute(select);
//		//List<Row> rows = scds.getSession().executeAsync(statement);		
//		//List<Row> rows = scds.getSession().execute(select).all();
//		
//		while(true){
//			Row delrow = result.one();
//			if(delrow == null)
//				break;
//			Where dQuery = QueryBuilder.delete()
//					.from(KEYSPACE, COLUMNFAMILY_JUSTIFICATIONS)
//					.where(QueryBuilder.eq(COLUMN_SUB, delrow.getLong(CassandraDB.COLUMN_SUB)))
//					.and(QueryBuilder.eq(COLUMN_PRE, delrow.getLong(CassandraDB.COLUMN_PRE)))
//					.and(QueryBuilder.eq(COLUMN_OBJ, delrow.getLong(CassandraDB.COLUMN_OBJ)))
//					.and(QueryBuilder.eq(COLUMN_IS_LITERAL, delrow.getBool(COLUMN_IS_LITERAL)));
//			System.out.println(delrow);
//			session.execute(dQuery);
//		}		
		
//		Where dQuery = QueryBuilder.delete()
//				.from(KEYSPACE, COLUMNFAMILY_JUSTIFICATIONS)
//				.where(QueryBuilder.eq(COLUMN_RULE, ByteBufferUtil.bytes(0)));
//				scds.getSession().execute(dQuery);
		
//		scds.close();		
		
//	}
	
	//create by LiYang
//	public static void createReasonTable(){
//		SimpleClientDataStax scds = new SimpleClientDataStax();
//		scds.connect(DEFAULT_HOST);
//		//Statement st = QueryBuilder
//		
//		for (int i = 1; i <= 7; i++ ){			
//			System.out.println("Select Primary Key");
//			//modified select partition key and delete using partition key
//			Statement select = QueryBuilder.select()
//					.all()
//					.from(KEYSPACE, COLUMNFAMILY_JUSTIFICATIONS)
//					.where(QueryBuilder.eq(COLUMN_INFERRED_STEPS, i));
//			select.setFetchSize(100);
//			ResultSet result = scds.getSession().execute(select);
//			
//			Session session = scds.getSession();
//			while(true){
//				Row insertrow = result.one();
//				if(insertrow == null)
//					break;
//				Insert insert = QueryBuilder
//						.insertInto(KEYSPACE, COLUMNFAMILY_JUSTIFICATIONS)
//						.value(COLUMN_SUB, insertrow.getLong(CassandraDB.COLUMN_SUB))
//						.value(COLUMN_PRE, insertrow.getLong(CassandraDB.COLUMN_PRE))
//						.value(COLUMN_OBJ, insertrow.getLong(CassandraDB.COLUMN_OBJ))
//						.value(COLUMN_IS_LITERAL, insertrow.getBool(COLUMN_IS_LITERAL))
//						.value(COLUMN_TRIPLE_TYPE, insertrow.getLong(CassandraDB.COLUMN_TRIPLE_TYPE))
//						.value(COLUMN_SUB, insertrow.getLong(CassandraDB.COLUMN_SUB));
//						
//			}	
//		}
//	}
	
	public static void main(String[] args) {
		try {
			CassandraDB db = new CassandraDB();		
			db.init();		
//			db.createIndexOnTripleType();
//			db.createIndexOnRule();
//			db.createIndexOnInferredSteps();
//			db.createIndexOnTransitiveLevel();
//			db.insertResources(100, "Hello World!");
			Set<Long> schemaTriples = new HashSet<Long>();
			Set<Integer> filters = new HashSet<Integer>();
			filters.add(TriplesUtils.SCHEMA_TRIPLE_SUBPROPERTY);
			db.loadSetIntoMemory(schemaTriples, filters, 0);
			//db.loadMapIntoMemory(filters, inverted)
						
			System.out.println(schemaTriples);
			
			//modified 2015/5/19
			System.out.println("Transitive: " + db.getRowCountAccordingTripleType(TriplesUtils.TRANSITIVE_TRIPLE));
			
	        System.exit(0);
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (InvalidRequestException e) {
			e.printStackTrace();
		} catch (UnavailableException e) {
			e.printStackTrace();
		} catch (TimedOutException e) {
			e.printStackTrace();
		} catch (SchemaDisagreementException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
