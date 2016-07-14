package prejustification;
import cn.edu.neu.mitt.mrj.io.dbs.CassandraDB;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.mapreduce.Mapper;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;



public class SelectInferRowsMap extends Mapper<ByteBuffer, Row, Map<String, ByteBuffer>, ByteBuffer> {
	private Cluster cluster;
	private Session session;

	
	public void map(ByteBuffer key, Row row, Context context) throws IOException, InterruptedException{
		SimpleStatement statement = new SimpleStatement("SELECT * FROM mrjks.justifications");
		statement.setFetchSize(100);
		ResultSet results = session.execute(statement);
		
		System.out.println("---------MAP----------");
		Map<String, ByteBuffer> keys = new HashMap<>();
		ByteBuffer inferredsteps;
		for (Row rows : results){
			if (rows.getInt(CassandraDB.COLUMN_RULE) != 0) {
				keys.put(CassandraDB.COLUMN_SUB, ByteBufferUtil.bytes(rows.getLong(CassandraDB.COLUMN_SUB)));
				keys.put(CassandraDB.COLUMN_PRE, ByteBufferUtil.bytes(rows.getLong(CassandraDB.COLUMN_PRE)));
				keys.put(CassandraDB.COLUMN_OBJ, ByteBufferUtil.bytes(rows.getLong(CassandraDB.COLUMN_SUB)));
				keys.put(CassandraDB.COLUMN_IS_LITERAL, ByteBufferUtil.bytes(rows.getLong(CassandraDB.COLUMN_IS_LITERAL)));
				keys.put(CassandraDB.COLUMN_TRIPLE_TYPE, ByteBufferUtil.bytes(rows.getLong(CassandraDB.COLUMN_TRIPLE_TYPE)));
				keys.put(CassandraDB.COLUMN_RULE, ByteBufferUtil.bytes(rows.getLong(CassandraDB.COLUMN_RULE)));
				keys.put(CassandraDB.COLUMN_V1, ByteBufferUtil.bytes(rows.getLong(CassandraDB.COLUMN_V1)));
				keys.put(CassandraDB.COLUMN_V2, ByteBufferUtil.bytes(rows.getLong(CassandraDB.COLUMN_V2)));
				keys.put(CassandraDB.COLUMN_V3, ByteBufferUtil.bytes(rows.getLong(CassandraDB.COLUMN_V3)));
				inferredsteps = ByteBufferUtil.bytes(rows.getInt(CassandraDB.COLUMN_INFERRED_STEPS));
				context.write(keys, inferredsteps);
			}
		}
	}
	
	public void setup(Context context) throws IOException, InterruptedException{
		
		cluster = Cluster.builder().addContactPoint(cn.edu.neu.mitt.mrj.utils.Cassandraconf.host).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("-------Connected to cluster: %s\n", metadata.getClusterName());
		session = cluster.connect();
		
        String cquery1 = "CREATE TABLE IF NOT EXISTS " + CassandraDB.KEYSPACE + "."  + "ruleiszero" + 
                " ( " + 
                CassandraDB.COLUMN_SUB + " bigint, " +			// partition key
                CassandraDB.COLUMN_PRE + " bigint, " +			// partition key
                CassandraDB.COLUMN_OBJ + " bigint, " +			// partition key
                CassandraDB.COLUMN_IS_LITERAL + " boolean, " +	// partition key
                CassandraDB.COLUMN_TRIPLE_TYPE + " int, " +
                CassandraDB.COLUMN_RULE + " int, " +
                CassandraDB.COLUMN_V1 + " bigint, " +
                CassandraDB.COLUMN_V2 + " bigint, " +
                CassandraDB. COLUMN_V3 + " bigint, " +
				CassandraDB.COLUMN_INFERRED_STEPS + " int, " +		// this is the only field that is not included in the primary key
                "   PRIMARY KEY ((" + CassandraDB.COLUMN_SUB + ", " + CassandraDB.COLUMN_PRE + ", " + CassandraDB.COLUMN_OBJ +  ", " + CassandraDB.COLUMN_IS_LITERAL + "), " +
                CassandraDB.COLUMN_TRIPLE_TYPE + ", " + CassandraDB.COLUMN_RULE + ", " + CassandraDB.COLUMN_V1 + ", " + CassandraDB.COLUMN_V2 + ", " +  CassandraDB.COLUMN_V3 +  
                " ) ) ";
        session.execute(cquery1);
	}
	
	
}
