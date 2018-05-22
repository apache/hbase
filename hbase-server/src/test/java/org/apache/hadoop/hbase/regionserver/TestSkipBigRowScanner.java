package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestSkipBigRowScanner {
    private static final Log LOG = LogFactory.getLog(TestJoinedScanners.class);

    private static final byte[] cf_name = Bytes.toBytes("a");
    private static final byte[] col_name = Bytes.toBytes("a");

    private static int valueWidth = 2 * 1024 * 1024;

    @Rule
    public TestName name = new TestName();

    @Test
    public void testJoinedScanners() throws Exception {
        String dataNodeHosts[] = new String[] { "host1", "host2", "host3" };
        int regionServersCount = 3;

        HBaseTestingUtility htu = new HBaseTestingUtility();

        final int DEFAULT_BLOCK_SIZE = 1024*1024;
        htu.getConfiguration().setLong("dfs.blocksize", DEFAULT_BLOCK_SIZE);
        htu.getConfiguration().setInt("dfs.replication", 1);
        htu.getConfiguration().setLong("hbase.hregion.max.filesize", 322122547200L);
        htu.getConfiguration().setLong(HConstants.TABLE_MAX_ROWSIZE_KEY,
                 1024 * 1024L);

        MiniHBaseCluster cluster = null;

        try {
            cluster = htu.startMiniCluster(1, regionServersCount, dataNodeHosts);

            final TableName tableName = TableName.valueOf(name.getMethodName());

            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(cf_name).build();
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
            htu.getAdmin().createTable(tableDescriptor);
            Table ht = htu.getConnection().getTable(tableName);

            byte [] val_large = new byte[valueWidth];

            List<Put> puts = new ArrayList<>();
            Put put = new Put(Bytes.toBytes("0"));
            put.addColumn(cf_name, col_name, val_large);
            puts.add(put);

            put = new Put(Bytes.toBytes("1"));
            put.addColumn(cf_name, col_name, Bytes.toBytes("small"));
            puts.add(put);

            put = new Put(Bytes.toBytes("2"));
            put.addColumn(cf_name, col_name, val_large);
            puts.add(put);

            ht.put(puts);
            puts.clear();

            Scan scan = new Scan();
            scan.addColumn(cf_name, col_name);
            ResultScanner result_scanner = ht.getScanner(scan);
            Result res;
            long rows_count = 0;
            //Only 1 row
            while ((res = result_scanner.next()) != null) {
                Assert.assertEquals("1",Bytes.toString(res.getRow()));
                rows_count++;
            }

            Assert.assertEquals(1, rows_count);
            result_scanner.close();
            ht.close();
        } finally {
            if (cluster != null) {
                htu.shutdownMiniCluster();
            }
        }
    }
}
