package org.apache.hadoop.hbase.security.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({ SecurityTests.class, LargeTests.class })
@SuppressWarnings("deprecation")
public class TestReadOnlyController {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReadOnlyController.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAccessController.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static TableName TEST_TABLE = TableName.valueOf("readonlytesttable");
  private static byte[] TEST_FAMILY = Bytes.toBytes("readonlytablecolfam");
  private static Configuration conf;
  private static Connection connection;

  private static RegionServerCoprocessorEnvironment RSCP_ENV;

  private static Table TestTable;
  @Rule
  public TestName name = new TestName();

  @Rule
  public ExpectedException exception = ExpectedException.none();
  @BeforeClass
  public static void beforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    // Only try once so that if there is failure in connection then test should fail faster
    conf.setInt("hbase.ipc.client.connect.max.retries", 1);
    // Shorter session timeout is added so that in case failures test should not take more time
    conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 1000);
    // Enable ReadOnly mode for the cluster
    conf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
    // Add the ReadOnlyController coprocessor for region server to interrupt any write operation
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, ReadOnlyController.class.getName());
    // Add the ReadOnlyController coprocessor to for master to interrupt any write operation
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, ReadOnlyController.class.getName());
    try{
      // Start the test cluster
      TEST_UTIL.startMiniCluster(2);
      // Get connection to the HBase
      connection = ConnectionFactory.createConnection(conf);
      // Create a test table
      TestTable = TEST_UTIL.createTable(TEST_TABLE, TEST_FAMILY);
    } catch (Exception e) {
      // Do cleanup
      // Delete the created table
      TEST_UTIL.deleteTable(TEST_TABLE);
      // Cleanup for Connection and HBase cluster
      connection.close();
      TEST_UTIL.shutdownMiniCluster();
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    // Delete the created table
    TEST_UTIL.deleteTable(TEST_TABLE);
    // Cleanup for Connection and HBase cluster
    connection.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testPut() throws IOException {
    final byte[] row1 = Bytes.toBytes("row1");
    final byte[] value = Bytes.toBytes("abcd");
    // Put a row in the table which should throw and exception
    Put put = new Put(row1);
    put.addColumn(TEST_FAMILY, null, value);
    TestTable.put(put);
    // This should throw IOException
    exception.expect(IOException.class);
  }
}
