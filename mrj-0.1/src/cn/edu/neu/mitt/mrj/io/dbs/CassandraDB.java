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


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gibeo_000
 *
 */
public class CassandraDB {
    private static final Logger logger = LoggerFactory.getLogger(CassandraDB.class);
    public static final String KEYSPACE = "mrjks";	// mr.j keyspace
    public static final String COLUMN_FAMILY_JUSTIFICATIONS = "justifications";	// mr.j keyspace
    public static final String COLUMN_FAMILY_RESOURCES = "resources";	// mr.j keyspace
    public static final String COLUMN_SUB = "sub";	// mrjks.justifications.sub
    public static final String COLUMN_PRE = "pre";	// mrjks.justifications.pre
    public static final String COLUMN_OBJ = "obj";	// mrjks.justifications.obj
    public static final String COLUMN_ID = "id";	// mrjks.resources.id
    public static final String COLUMN_LABEL = "label";	// mrjks.resources.label

	
	private Cassandra.Iface client;

    private static Cassandra.Iface createConnection() throws TTransportException{
        if (System.getProperty("cassandra.host") == null || System.getProperty("cassandra.port") == null){
            logger.warn("cassandra.host or cassandra.port is not defined, using default");
        }
        return createConnection(System.getProperty("cassandra.host", "localhost"),
                                Integer.valueOf(System.getProperty("cassandra.port", "9160")));
    }

    private static Cassandra.Client createConnection(String host, Integer port) throws TTransportException {
        TSocket socket = new TSocket(host, port);
        TTransport trans = new TFramedTransport(socket);
        trans.open();
        TProtocol protocol = new TBinaryProtocol(trans);

        return new Cassandra.Client(protocol);
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

    private static void setupTable(Cassandra.Iface client)  
            throws InvalidRequestException, 
            UnavailableException, 
            TimedOutException, 
            SchemaDisagreementException, 
            TException {
    	
        String query = "CREATE TABLE " + KEYSPACE + "."  + COLUMN_FAMILY_JUSTIFICATIONS + 
                          " ( sub bigint," +
                          "   pre bigint, " +
                          "   obj bigint, " +
                          "   PRIMARY KEY ((sub, pre, obj)) ) ";

        try {
            logger.info("set up table " + COLUMN_FAMILY_JUSTIFICATIONS);
            client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
        } catch (InvalidRequestException e) {
            logger.error("failed to create table " + KEYSPACE + "."  + COLUMN_FAMILY_JUSTIFICATIONS, e);
        }

        query = "CREATE TABLE " + KEYSPACE + "."  + COLUMN_FAMILY_RESOURCES + 
                " ( id bigint," +
                "   label text," +
                "   PRIMARY KEY (id) ) ";

        try {
            logger.info("set up table " + COLUMN_FAMILY_RESOURCES);
            client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (InvalidRequestException e) {
            logger.error("failed to create table " + KEYSPACE + "."  + COLUMN_FAMILY_RESOURCES, e);
        }
    }
    
    
    
	public CassandraDB() throws TTransportException {
		client = createConnection();
	}
	
	
	public CassandraDB(String host, Integer port) throws TTransportException {
		client = createConnection(host, port);
	}
	
	public void init() throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException{
	        setupKeyspace(client);
	        client.set_keyspace(KEYSPACE);
	        setupTable(client);
	}
	
	public Cassandra.Iface getDBClient(){
		return client;
	}
	
	
	public void insertResources(long id, String label) throws InvalidRequestException, TException{
        String query = "INSERT INTO " + COLUMN_FAMILY_RESOURCES +  
                "(id, label) " +
                " values (?, ?) ";
        List<ByteBuffer> args = new ArrayList<ByteBuffer>();
        args.add(ByteBufferUtil.bytes(id));
        args.add(ByteBufferUtil.bytes(label));
        CqlPreparedResult p_result = client.prepare_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE);
        CqlResult result = client.execute_prepared_cql3_query(p_result.itemId, args, ConsistencyLevel.ANY);
        logger.info("Number of results: " + result.getNum());
	}

	public void insertJustifications(long sub, long pre, long obj, short rule, long var1, long var2, long var3) 
			throws InvalidRequestException, TException{
        String query = "INSERT INTO " + COLUMN_FAMILY_JUSTIFICATIONS +  
                "(id, line) " +
                " values (?, ?) ";
        
        CqlPreparedResult result = client.prepare_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE);
        ByteBufferUtil.bytes(rule);
        
//		client.execute_prepared_cql3_query(result.itemId, arg1, ConsistencyLevel.ANY);
	}
	
	
	
	public static void main(String[] args) {
		try {
			CassandraDB db = new CassandraDB("localhost", 9160);
			db.init();
			db.insertResources(100, "Hello World!");
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
		}
	}

}
