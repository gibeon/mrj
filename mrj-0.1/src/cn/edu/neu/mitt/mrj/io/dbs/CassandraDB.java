/**
 * Project Name: mrj-0.1
 * File Name: DB.java
 * @author Gang Wu
 * 2014脛锚10脭脗11脠脮 脧脗脦莽2:39:42
 * 
 * Description: 
 * TODO
 */
package cn.edu.neu.mitt.mrj.io.dbs;


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

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.TupleType;
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
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.data.Triple;
import cn.edu.neu.mitt.mrj.data.TripleSource;
import cn.edu.neu.mitt.mrj.utils.TriplesUtils;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
//modified  cassandra java 2.0.5
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;


//modified 
import org.apache.cassandra.thrift.CassandraServer;


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
    public static final String COLUMN_SUB = "sub";	// mrjks.justifications.sub
    public static final String COLUMN_PRE = "pre";	// mrjks.justifications.pre
    public static final String COLUMN_OBJ = "obj";	// mrjks.justifications.obj
    public static final String COLUMN_TRIPLE_TYPE = "tripletype" ;	// mrjks.justifications.tripletype
    public static final String COLUMN_IS_LITERAL = "isliteral" ;	// mrjks.justifications.isliteral
    public static final String COLUMN_INFERRED_STEPS = "inferredsteps" ;	// mrjks.justifications.inferredsteps    
    public static final String COLUMN_RULE = "rule"; // mrjks.justifications.rule
    public static final String COLUMN_V1 = "v1"; // mrjks.justifications.rule
    public static final String COLUMN_V2 = "v2"; // mrjks.justifications.rule
    public static final String COLUMN_V3 = "v3"; // mrjks.justifications.rule
    public static final String COLUMN_ID = "id";	// mrjks.resources.id
    public static final String COLUMN_LABEL = "label";	// mrjks.resources.label
    public static final String COLUMN_JUSTIFICATION = "justification";	//mrjks.results.justification
    public static final String COLUMN_STEP = "step";	// mrjks.results.step
    
    public static final String DEFAULT_HOST = cn.edu.neu.mitt.mrj.utils.Cassandraconf.host;
    public static final String DEFAULT_PORT = "9160";	// in version 2.1.2, cql3 port is 9042
    
	public static final String CQL_PAGE_ROW_SIZE = "10000";	//3  modified by liyang

    
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
        return createConnection(System.getProperty("cassandra.host", DEFAULT_HOST),
                                Integer.valueOf(System.getProperty("cassandra.port", DEFAULT_PORT)));
    }

    
    
    private static TSocket socket = null;
    private static TTransport trans = null;
    private static Cassandra.Client client1 = null;
    private static Cassandra.Client createConnection(String host, Integer port) throws TTransportException {
        socket = new TSocket(host, port);
        trans = new TFramedTransport(socket);
        trans.open();
        TProtocol protocol = new TBinaryProtocol(trans);

        if (client1 != null){
        	return client1;
        }     
        client1 = new Cassandra.Client(protocol);        
        //Modified 2015/5/25
        return client1;
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
                              " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}"; 

            client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);

            String verifyQuery = "select count(*) from system.peers";
            CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes(verifyQuery), Compression.NONE, ConsistencyLevel.ONE);

            long magnitude = ByteBufferUtil.toLong(result.rows.get(0).columns.get(0).value);
            try {
                Thread.sleep(1000 * magnitude);
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }
    }

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
                COLUMN_INFERRED_STEPS + " int, " +		// this is the only field that is not included in the primary key
                "   PRIMARY KEY ((" + COLUMN_SUB + ", " + COLUMN_PRE + ", " + COLUMN_OBJ +  ", " + COLUMN_IS_LITERAL + "), " +
                COLUMN_TRIPLE_TYPE + ", " + COLUMN_RULE + ", " + COLUMN_V1 + ", " + COLUMN_V2 + ", " +  COLUMN_V3 +  
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
        		COLUMN_ID + " uuid, " +
                COLUMN_JUSTIFICATION + " set<frozen <tuple<bigint, bigint, bigint>>>, " +
                "   PRIMARY KEY (" + COLUMN_ID + ") ) ";
        
        try {
            logger.info("set up table " + COLUMNFAMILY_RESULTS);
            client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (InvalidRequestException e) {
            logger.error("failed to create table " + KEYSPACE + "."  + COLUMNFAMILY_RESULTS, e);
        }

    }
    
    
    
	public CassandraDB() throws TTransportException {
		client = createConnection();
	}
	
	
	public CassandraDB(String host, Integer port) throws TTransportException {
		client = createConnection(host, port);
	}
	
	public void CassandraDBClose(){
		this.close();
	}
	
	public void init() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException{
	        setupKeyspace(client);
	        client.set_keyspace(KEYSPACE);
	        setupTables(client);
	}
	
	public Cassandra.Iface getDBClient(){
		return client;
	}
	
	//TriplesUtils.SYNONYMS_TABLE
	//TriplesUtils.TRANSITIVE_TRIPLE
	//TriplesUtils.DATA_TRIPLE_SAME_AS
	/**
	 * Get the row count according to the triple type.
	 * @return row count.
	 */
	public long getRowCountAccordingTripleType(int tripletype){
		String query = "SELECT COUNT(*) FROM " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS + 
				" WHERE " + COLUMN_TRIPLE_TYPE + " = " + tripletype;

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
        logger.info("Number of results: " + result.getNum());
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
		long v1 = row.getLong(CassandraDB.COLUMN_V1);
		long v2 = row.getLong(CassandraDB.COLUMN_V2);
		long v3 = row.getLong(CassandraDB.COLUMN_V3);
		int rule = row.getInt(CassandraDB.COLUMN_RULE);

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
	
	public static void writeJustificationToMapReduceContext(
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
        context.write(keys, variables);
		
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
	
	//modified  cassandra java 2.0.5
	
	public static Set<Set<TupleValue>> getJustifications() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException, IOException, ClassNotFoundException, RequestExecutionException{
		Set<Set<TupleValue>> results = new HashSet<Set<TupleValue>>();
		
//		String query = "SELECT " + COLUMN_JUSTIFICATION + " FROM " + KEYSPACE + "."  + COLUMNFAMILY_RESULTS;
		SimpleClientDataStax scds = new SimpleClientDataStax();
		scds.connect(DEFAULT_HOST);
		Statement statement = QueryBuilder.select().all().from(KEYSPACE, COLUMNFAMILY_RESULTS);
		List<Row> rows = scds.getSession().execute(statement).all();

		for (Row row : rows){
			Set<TupleValue> testResult = row.getSet(COLUMN_JUSTIFICATION, TupleValue.class);
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
		
		String query = "SELECT * FROM " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS + " WHERE " + 
				COLUMN_SUB + "=? AND " + COLUMN_PRE + "=? AND " + COLUMN_OBJ + "=? AND " + COLUMN_IS_LITERAL + "=?";
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
        String query = "SELECT " + COLUMN_SUB + ", " + COLUMN_OBJ + ", " + COLUMN_INFERRED_STEPS +
        		" FROM " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS +  
                " WHERE " + COLUMN_TRIPLE_TYPE + " = ? ";
//        System.out.println(query);
        
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

        	}
        }
		
		logger.debug("Time for CassandraDB's loadSetIntoMemory " + (System.currentTimeMillis() - startTime));
		return schemaChanged;
	}
	
	
	public Map<Long, Collection<Long>> loadMapIntoMemory(Set<Integer> filters) throws IOException, InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException {
		return loadMapIntoMemory(filters, false);
	}
	
	// 返回的key就是triple的subject，value是object
	public Map<Long, Collection<Long>> loadMapIntoMemory(Set<Integer> filters, boolean inverted) throws IOException, InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException {
		long startTime = System.currentTimeMillis();

		logger.info("In CassandraDB's loadMapIntoMemory");

		Map<Long, Collection<Long>> schemaTriples = new HashMap<Long, Collection<Long>>();

		// Require an index created on COLUMN_TRIPLE_TYPE column
        String query = "SELECT " + COLUMN_SUB + ", " + COLUMN_OBJ + ", " + COLUMN_INFERRED_STEPS +
        		" FROM " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS +  
                " WHERE " + COLUMN_TRIPLE_TYPE + " = ? ";
        
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
		
		logger.debug("Time for CassandraDB's loadMapIntoMemory " + (System.currentTimeMillis() - startTime));

		return schemaTriples;
	}	


	// Created index on COLUMN_TRIPLE_TYPE column
	public void createIndexOnTripleType() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException{
		String query = "CREATE INDEX ON " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS + "(" + COLUMN_TRIPLE_TYPE + ")";
		client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
	}
	
	public void createIndexOnRule() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException{
		String query = "CREATE INDEX ON " + KEYSPACE + "."  + COLUMNFAMILY_JUSTIFICATIONS + "(" + COLUMN_RULE + ")";
		client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
	}

	
	
	public static void main(String[] args) {
		try {
			CassandraDB db = new CassandraDB(cn.edu.neu.mitt.mrj.utils.Cassandraconf.host, 9160);
			db.init();
			db.createIndexOnTripleType();
			db.createIndexOnRule();
//			db.insertResources(100, "Hello World!");
			Set<Long> schemaTriples = new HashSet<Long>();
			Set<Integer> filters = new HashSet<Integer>();
			filters.add(TriplesUtils.SCHEMA_TRIPLE_SUBPROPERTY);
			db.loadSetIntoMemory(schemaTriples, filters, 0);
			
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
