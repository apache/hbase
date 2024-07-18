package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.*;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestScannerLeaseCount {
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final TableName TABLE_NAME = TableName.valueOf("ScannerLeaseCount");
  private static final byte[] FAM = Bytes.toBytes("Fam");

  private static Connection CONN;
  private static Table TABLE;

  private static SingleProcessHBaseCluster cluster;

  private static final RegionServerRpcQuotaManager QUOTA_MANAGER = mock(RegionServerRpcQuotaManager.class);

  @BeforeClass
  public static void setUp() throws Exception {
    StartTestingClusterOption option =
      StartTestingClusterOption.builder().rsClass(MockedQuotaManagerRegionServer.class).build();
    cluster = UTIL.startMiniCluster(option);
    UTIL.getAdmin().createTable(TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAM)).build());
    Configuration conf = new Configuration(UTIL.getConfiguration());
    CONN = ConnectionFactory.createConnection(conf);
    TABLE = CONN.getTable(TABLE_NAME);
    UTIL.loadTable(TABLE, FAM);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      TABLE.close();
    } catch (Exception ignore) {}
    try {
      CONN.close();
    } catch (Exception ignore) {}
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void itIncreasesScannerCount() throws Exception {
    when(QUOTA_MANAGER.checkScanQuota(any(), any(), anyByte(), anyByte(), anyByte())).thenReturn(NoopOperationQuota.get());
    try(ResultScanner ignore = TABLE.getScanner(new Scan())) {
      int count = countScanners();
      assertSame(1, count);
    }
  }

  public static final class MockedQuotaManagerRegionServer extends SingleProcessHBaseCluster.MiniHBaseClusterRegionServer {

    public MockedQuotaManagerRegionServer(Configuration conf)
      throws IOException, InterruptedException {
      super(conf);
    }
  }

  private int countScanners() {
    int serverNum = cluster.getNumLiveRegionServers();
    int count = 0;
    for (int i = 0; i < serverNum; i++) {
      HRegionServer server = cluster.getRegionServer(i);
      count += server.getRpcServices().getScannersCount();
    }
    return count;
  }
}
