package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, MediumTests.class })
public class TestConnectionAttributes {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestConnectionAttributes.class);

  private static final Map<String, byte[]> CONNECTION_ATTRIBUTES = new HashMap<>();
  static {
    CONNECTION_ATTRIBUTES.put("clientId", Bytes.toBytes("foo"));
  }
  private static final byte[] FAMILY = Bytes.toBytes("0");
  private static final TableName TABLE_NAME = TableName.valueOf("testConnectionAttributes");

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static SingleProcessHBaseCluster cluster;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = TEST_UTIL.startMiniCluster(1);
    Table table = TEST_UTIL.createTable(TABLE_NAME,
      new byte[][] { FAMILY }, 1, HConstants.DEFAULT_BLOCKSIZE,
      TestConnectionAttributes.AttributesCoprocessor.class.getName());
    table.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    cluster.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testConnectionHeaderOverwrittenAttributesRemain() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    try (Connection conn = ConnectionFactory.createConnection(conf, null,
      AuthUtil.loginClient(conf), CONNECTION_ATTRIBUTES); Table table = conn.getTable(TABLE_NAME)) {

      // submit a 300 byte rowkey here to encourage netty's allocator to overwrite the connection
      // header
      byte[] bytes = new byte[300];
      new Random().nextBytes(bytes);
      Result result = table.get(new Get(bytes));

      assertEquals(CONNECTION_ATTRIBUTES.size(), result.size());
      for (Map.Entry<String, byte[]> attr : CONNECTION_ATTRIBUTES.entrySet()) {
        byte[] val = result.getValue(FAMILY, Bytes.toBytes(attr.getKey()));
        assertEquals(Bytes.toStringBinary(attr.getValue()), Bytes.toStringBinary(val));
      }
    }
  }

  public static class AttributesCoprocessor implements RegionObserver, RegionCoprocessor {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get,
      List<Cell> result) throws IOException {
      RpcCall rpcCall = RpcServer.getCurrentCall().get();
      for (Map.Entry<String, byte[]> attr : rpcCall.getConnectionAttributes().entrySet()) {
        result.add(c.getEnvironment().getCellBuilder().clear().setRow(get.getRow())
          .setFamily(FAMILY).setQualifier(Bytes.toBytes(attr.getKey()))
          .setValue(attr.getValue()).setType(Cell.Type.Put).setTimestamp(1).build());
      }
      result.sort(CellComparator.getInstance());
      c.bypass();
    }
  }
}
