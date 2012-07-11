package eu.scapeproject.akubra;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HBaseIdIterator implements Iterator<URI>{
    
    private final HTable table;
    private final Iterator<Result> iterator;
    private final URI storeId;
    
    public HBaseIdIterator(HTable table,URI storeId) {
        this.table=table;
        this.storeId=storeId;
        try {
            iterator=table.getScanner(HBaseBlobStore.DATA_FAMILY, HBaseBlobStore.DEFAULT_QUALIFIER).iterator();
        } catch (IOException e) {
            throw new RuntimeException(e.getLocalizedMessage(), e);
        }
    }

    public boolean hasNext() {
        return iterator.hasNext();
    }
    
    public URI next() {
        return URI.create(this.storeId.toASCIIString() + "/" + iterator.next().getRow());
    }

    public void remove() {
        throw new UnsupportedOperationException("remove is not implemented");
    }

}
