package org.apache.hadoop.hbase.regionserver.snapshot;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.procedure.ProcedureMember;
import org.apache.hadoop.hbase.procedure.RegionServerProcedureManagerHost;
import org.apache.hadoop.hbase.procedure.Subprocedure;
import org.apache.hadoop.hbase.procedure.SubprocedureFactory;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.io.IOException;
import java.lang.reflect.Field;
import static org.junit.Assert.*;

@Category({MediumTests.class, RegionServerTests.class })
public class TestRegionServerSnapshotManager {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionServerSnapshotManager.class);
  private static HBaseTestingUtility TEST_UTIL;
  private static Connection connection;
  private static Admin admin;
  private static Boolean hasError = false;
  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.startMiniCluster(2);
    TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster();
    TEST_UTIL.waitUntilAllRegionsAssigned(TableName.NAMESPACE_TABLE_NAME);
    connection = TEST_UTIL.getConnection();
    admin = connection.getAdmin();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private int createAndLoadTable(TableName tableName) throws IOException {
    TEST_UTIL.createTable(tableName,new byte[][]{Bytes.toBytes("fam")});
    TEST_UTIL.loadTable(connection.getTable(tableName), Bytes.toBytes("fam"));
    return TEST_UTIL.countRows(tableName);
  }

  private Object setAndGetField(Object object, String field, Object value)
    throws IllegalAccessException, NoSuchFieldException {
    Field f = null;
    try {
      f = object.getClass().getField(field);
    } catch (NoSuchFieldException e) {
      f = object.getClass().getSuperclass().getField(field);
    }
    f.setAccessible(true);
    if (value != null) {
      f.set(object, value);
    }
    return f.get(object);
  }

  private void setRSSnapshotProcManagerMock(HRegionServer regionServer, boolean hasRegion)
    throws NoSuchFieldException, IllegalAccessException {
    RegionServerProcedureManagerHost rspmHost = (RegionServerProcedureManagerHost) setAndGetField(regionServer, "rspmHost", null);
    RegionServerSnapshotManager rsManager = rspmHost.getProcedureManagers().stream().filter(v -> v instanceof RegionServerSnapshotManager)
      .map(v -> (RegionServerSnapshotManager)v).findAny().get();
    ProcedureMember procedureMember = (ProcedureMember) setAndGetField(rsManager, "member", null);
    setAndGetField(procedureMember, "builder", new SubprocedureFactory() {
      @Override
      public Subprocedure buildSubprocedure(String procName, byte[] procArgs) {
        SnapshotDescription snapshot = null;
        try {
          snapshot = SnapshotDescription.parseFrom(procArgs);
        } catch (InvalidProtocolBufferException e) {
          throw new IllegalArgumentException("Could not read snapshot information from request.");
        }
        Subprocedure subprocedure = rsManager.buildSubprocedure(snapshot);
        hasError |= (hasRegion && subprocedure == null || !hasRegion && subprocedure != null);
        return subprocedure;
      }
    });
  }

  @Test
  public void testInvalidSubProcedure()
    throws IOException, NoSuchFieldException, IllegalAccessException {
    TableName tableName = TableName.valueOf("test_table");
    int count = createAndLoadTable(tableName);
    ServerName serverName = connection.getRegionLocator(tableName).getAllRegionLocations().stream()
      .map(v -> v.getServerName()).findAny().get();
    HRegionServer regionServer0 = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    HRegionServer regionServer1 = TEST_UTIL.getHBaseCluster().getRegionServer(1);
    setRSSnapshotProcManagerMock(regionServer0, regionServer0.getServerName().equals(serverName));
    setRSSnapshotProcManagerMock(regionServer1, regionServer1.getServerName().equals(serverName));
    admin.snapshot("ss", tableName);
    assertFalse(hasError);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    admin.cloneSnapshot("ss", tableName);
    assertEquals(count, TEST_UTIL.countRows(tableName));
  }

}
