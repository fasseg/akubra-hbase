package eu.scapeproject.akubra;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class HBaseOutputStream extends OutputStream{
    private final HTable table;
    private byte[] key;
    private ByteArrayOutputStream bos=new ByteArrayOutputStream();
    private boolean closed=false;
    
    public HBaseOutputStream(HTable table, byte[] key) {
        super();
        this.table = table;
        this.key = key;
    }

    @Override
    public void write(int b) throws IOException {
        bos.write(b);
    }
    
    @Override
    public void flush() throws IOException {
        if (closed) {
            throw new IOException("Stream has been closed already");
        }
        Put p=new Put(key);
        p.add(HBaseBlobStore.DATA_FAMILY, HBaseBlobStore.DEFAULT_QUALIFIER, bos.toByteArray());
        this.table.put(p);
    }
    
    @Override
    public void close() throws IOException {
        if (closed) {
            throw new IOException("Stream has been closed already");
        }
        this.flush();
        this.closed=true;
        key=null;
        bos=null;
    }
}
