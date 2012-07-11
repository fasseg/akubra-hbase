package eu.scapeproject.akubra;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import javax.transaction.Transaction;

import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

public class HBaseBlobStore implements BlobStore{
    
    public static final byte[] DEFAULT_QUALIFIER="default".getBytes();
    public static final byte[] DATA_FAMILY="data".getBytes();
    private final String tableName;
    
    
    private final URI id;
    
    public HBaseBlobStore(URI id,String tableName) {
        super();
        this.id = id;
        this.tableName=tableName;
    }

    public String getTableName() {
        return tableName;
    }
    
    public URI getId() {
        return id;
    }

    public BlobStoreConnection openConnection(Transaction arg0, Map<String, String> arg1) throws UnsupportedOperationException, IOException {
        if (arg0 != null) {
            throw new UnsupportedOperationException("Transactions are not available");
        }
        try {
            return new HBaseBlobStoreConnection(this,tableName);
        }catch(Exception ex) {
            throw new RuntimeException("Unable o connect to HBase tables");
        }
    }
    
}

