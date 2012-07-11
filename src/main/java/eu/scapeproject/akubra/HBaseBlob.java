package eu.scapeproject.akubra;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;

import org.akubraproject.Blob;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.DuplicateBlobException;
import org.akubraproject.MissingBlobException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.metrics.HBaseInfo;

public class HBaseBlob implements Blob {
    
    
    private final HBaseBlobStoreConnection conn;
    private final URI id;
    private final Get get;
    

    public HBaseBlob(HBaseBlobStoreConnection conn,URI id) {
        super();
        this.conn = conn;
        this.id=id;
        this.get=new Get(id.toASCIIString().getBytes());
    }

    public void delete() throws IOException {
        Delete del=new Delete(id.toASCIIString().getBytes());
        this.conn.getTable().delete(del);
    }

    public boolean exists() throws IOException {
        return this.conn.getTable().exists(get);
    }

    public URI getCanonicalId() throws IOException {
        return id;
    }

    public BlobStoreConnection getConnection() {
        return conn;
    }

    public URI getId() {
        return id;
    }

    public long getSize() throws IOException, MissingBlobException {
        return this.conn.getTable().get(get).getValue(HBaseBlobStore.DATA_FAMILY, HBaseBlobStore.DEFAULT_QUALIFIER).length;
    }

    public Blob moveTo(URI arg0, Map<String, String> arg1) throws DuplicateBlobException, IOException, MissingBlobException, NullPointerException,
            IllegalArgumentException {
        HBaseBlob moved=new HBaseBlob(this.conn,arg0);
        OutputStream os=null;
        InputStream is=null;
        try{
            os=moved.openOutputStream(-1,false);
            is=openInputStream();
            IOUtils.copy(is,os);
            this.delete();
            return moved;
        } finally {
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(os);
        }
    }

    public InputStream openInputStream() throws IOException, MissingBlobException {
        return new ByteArrayInputStream(this.conn.getTable().get(get).getValue(HBaseBlobStore.DATA_FAMILY, HBaseBlobStore.DEFAULT_QUALIFIER));
    }

    public OutputStream openOutputStream(long arg0, boolean arg1) throws IOException, DuplicateBlobException {
        return new HBaseOutputStream(this.conn.getTable(),id.toASCIIString().getBytes());
    }

}
