package org.apache.hadoop.hbase.client.tableindexed.migration;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.tableindexed.IndexSpecification;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * One time migration account for index metadata change for 0.20. (Not handled by hbase migration).
 */
public class MoveIndexMetaData {

    private static final byte[] INDEXES_KEY = Bytes.toBytes("INDEXES");

    public static void main(final String[] args) {
        try {
            HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
            for (HTableDescriptor tableDesc : admin.listTables()) {
                if (!tableDesc.getIndexes().isEmpty()) {
                    admin.disableTable(tableDesc.getName());
                    writeToTable(tableDesc.getIndexes(), tableDesc);
                    admin.modifyTable(tableDesc.getName(), tableDesc);
                    admin.enableTable(tableDesc.getName());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeToTable(final Collection<IndexSpecification> indexes, final HTableDescriptor tableDesc) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        IndexSpecificationArray indexArray = new IndexSpecificationArray(indexes.toArray(new IndexSpecification[0]));

        try {
            indexArray.write(dos);
            dos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        tableDesc.setValue(INDEXES_KEY, baos.toByteArray());
    }
}
