package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableRegionLocatorWithRegionReplicaId {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableRegionLocatorWithRegionReplicaId.class);

  @Rule
  public TestName name = new TestName();

  private ExpectedException exception = ExpectedException.none();

  private final static HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final String ROW = "r1";
  private static final byte[] FAMILY = Bytes.toBytes("info");
  private static final int REGION_REPLICATION_COUNT = 2;
  // region replica id starts from 0
  private static final int NON_EXISTING_REGION_REPLICA_ID = REGION_REPLICATION_COUNT;
  private static Connection connection;
  private static AsyncConnection asyncConn;
  private static Admin admin;
  private TableName tableName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(1);
    connection = UTIL.getConnection();
    asyncConn = ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get();
    admin = UTIL.getAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    IOUtils.cleanup(null, admin);
    IOUtils.cleanup(null, connection);
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    tableName = TableName.valueOf(name.getMethodName());
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).build();
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(columnFamilyDescriptor)
        .setRegionReplication(REGION_REPLICATION_COUNT).build();
    admin.createTable(tableDescriptor);
    UTIL.waitTableAvailable(tableName);
    assertTrue(admin.tableExists(tableName));
    assertEquals(REGION_REPLICATION_COUNT, tableDescriptor.getRegionReplication());

    List<HRegion> regions = UTIL.getHBaseCluster().getRegions(tableName);
    assertEquals(REGION_REPLICATION_COUNT, regions.size());

    Table table = connection.getTable(tableName);
    Put put = new Put(Bytes.toBytes(ROW)).addColumn(FAMILY, Bytes.toBytes("q"),
      Bytes.toBytes("test_value"));
    table.put(put);
    admin.flush(tableName);

    Scan scan = new Scan();
    ResultScanner rs = table.getScanner(scan);
    rs.forEach(r -> assertEquals(ROW, Bytes.toString(r.getRow())));
  }

  @After
  public void tearDown() throws Exception {
    UTIL.deleteTable(tableName);
  }

  @Test
  public void testMetaTableRegionLocatorWithRegionReplicaId()
    throws ExecutionException, InterruptedException {
    AsyncTableRegionLocator locator = asyncConn.getRegionLocator(TableName.META_TABLE_NAME);
    CompletableFuture<HRegionLocation>
      future = locator.getRegionLocation(tableName.getName(), RegionReplicaUtil.DEFAULT_REPLICA_ID, true);
    HRegionLocation hrl = future.get();
    assertNotNull(hrl);
  }

  @Test
  public void testMetaTableRegionLocatorWithNonExistingRegionReplicaId() throws InterruptedException {
    AsyncTableRegionLocator locator = asyncConn.getRegionLocator(TableName.META_TABLE_NAME);
    CompletableFuture<HRegionLocation>
      future = locator.getRegionLocation(tableName.getName(), NON_EXISTING_REGION_REPLICA_ID, true);
    try {
      future.get();
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof DoNotRetryIOException);
      String message = "The specified region replica id '"
        + NON_EXISTING_REGION_REPLICA_ID + "' does not exist, the REGION_REPLICATION of this "
        + "table " + TableName.META_TABLE_NAME.getNameAsString() + " is "
        + TableDescriptorBuilder.DEFAULT_REGION_REPLICATION + ", "
        + "this means that the maximum region replica id you can specify is "
        + (TableDescriptorBuilder.DEFAULT_REGION_REPLICATION - 1) + ".";
      assertEquals(message, e.getCause().getMessage());
    }
  }

  @Test
  public void testTableRegionLocatorWithRegionReplicaId()
    throws ExecutionException, InterruptedException {
    AsyncTableRegionLocator locator = asyncConn.getRegionLocator(tableName);
    CompletableFuture<HRegionLocation>
      future = locator.getRegionLocation(Bytes.toBytes(ROW), RegionReplicaUtil.DEFAULT_REPLICA_ID, true);
    HRegionLocation hrl = future.get();
    assertNotNull(hrl);
  }

  @Test
  public void testTableRegionLocatorWithNonExistingRegionReplicaId() throws InterruptedException {
    AsyncTableRegionLocator locator = asyncConn.getRegionLocator(tableName);
    CompletableFuture<HRegionLocation> future = locator.getRegionLocation(Bytes.toBytes(ROW), NON_EXISTING_REGION_REPLICA_ID, true);
    try {
      future.get();
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof DoNotRetryIOException);
      String message = "The specified region replica id '"
        + NON_EXISTING_REGION_REPLICA_ID + "' does not exist, the REGION_REPLICATION of this "
        + "table " + tableName.getNameAsString() + " is " + REGION_REPLICATION_COUNT + ", "
        + "this means that the maximum region replica id you can specify is "
        + (REGION_REPLICATION_COUNT - 1) + ".";
      assertEquals(message, e.getCause().getMessage());
    }
  }
}
