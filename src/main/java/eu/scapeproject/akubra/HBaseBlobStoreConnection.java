package eu.scapeproject.akubra;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.UnsupportedIdException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

public class HBaseBlobStoreConnection implements BlobStoreConnection {

    private final HTable table;
    private final HBaseBlobStore store;
    private final HBaseAdmin admin;
    private final Configuration config;

    private boolean closed;

    public HBaseBlobStoreConnection(HBaseBlobStore store,String tableName) {
        super();
        try {
            this.config = HBaseConfiguration.create();
            this.config.clear();
            this.config.set("hbase.zookeeper.quorum", "127.0.0.1");
            System.out.println("getting admin connection for " + tableName);
            this.admin = new HBaseAdmin(this.config);
            System.out.println("getting table");
            this.table = getTable(tableName);
        }catch(IOException e) {
            throw new RuntimeException(e.getLocalizedMessage(),e);
        }
        this.store=store;
        closed = false;
    }

    public void close() {
        try {
            this.admin.close();
        }catch (IOException e) {
            throw new RuntimeException(e.getLocalizedMessage(),e);
        }
        closed = true;
    }

    public HTable getTable() {
        return table;
    }
    
    private HTable getTable(String name) throws IOException {
        if (closed) {
            throw new IOException("connection is closed");
        }
        try {
            if (!admin.tableExists(name)) {
                HTableDescriptor desc = new HTableDescriptor(name);
                HColumnDescriptor colDesc=new HColumnDescriptor(HBaseBlobStore.DATA_FAMILY);
                desc.addFamily(colDesc);
                admin.createTable(desc);
            }
            return new HTable(config, name);
        } catch (Exception e) {
            throw new RuntimeException(e.getLocalizedMessage(),e);
        }
    }

    public Blob getBlob(URI arg0, Map<String, String> arg1) throws IOException, UnsupportedIdException, UnsupportedOperationException {
        return new HBaseBlob(this, arg0);
    }

    public Blob getBlob(InputStream is, long arg1, Map<String, String> arg2) throws IOException, UnsupportedOperationException {
        HBaseBlob blob=new HBaseBlob(this,URI.create(this.store.getId().toASCIIString() + "/" + UUID.randomUUID().toString()));
        System.out.println("creating new blob " + blob.getId().toASCIIString());
        OutputStream os=null;
        try {
            os = blob.openOutputStream(arg1, false);
            IOUtils.copy(is, os);
            return blob;
        }finally {
            IOUtils.closeQuietly(os);
        }
    }

    public BlobStore getBlobStore() {
        return store;
    }

    public boolean isClosed() {
        return closed;
    }

    public Iterator<URI> listBlobIds(String arg0) throws IOException {
        return new HBaseIdIterator(getTable(), store.getId());
    }

    public void sync() throws IOException, UnsupportedOperationException {
        throw new UnsupportedOperationException("sync is not implemented");

    }

}
