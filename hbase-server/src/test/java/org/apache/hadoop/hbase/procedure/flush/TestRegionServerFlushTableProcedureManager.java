package org.apache.hadoop.hbase.procedure.flush;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.procedure.ProcedureMember;
import org.apache.hadoop.hbase.procedure.RegionServerProcedureManagerHost;
import org.apache.hadoop.hbase.procedure.Subprocedure;
import org.apache.hadoop.hbase.procedure.SubprocedureFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import static org.junit.Assert.*;

public class TestRegionServerFlushTableProcedureManager {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionServerFlushTableProcedureManager.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestRegionServerFlushTableProcedureManager.class);
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
    TEST_UTIL.createTable(tableName,new byte[][]{ Bytes.toBytes("fam")});
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
    RegionServerProcedureManagerHost
      rspmHost = (RegionServerProcedureManagerHost) setAndGetField(regionServer, "rspmHost", null);
    RegionServerFlushTableProcedureManager rsManager = rspmHost.getProcedureManagers().stream().filter(v -> v instanceof RegionServerFlushTableProcedureManager)
      .map(v -> (RegionServerFlushTableProcedureManager)v).findAny().get();
    ProcedureMember procedureMember = (ProcedureMember) setAndGetField(rsManager, "member", null);
    setAndGetField(procedureMember, "builder", new SubprocedureFactory() {
      @Override
      public Subprocedure buildSubprocedure(String procName, byte[] procArgs) {
        String family = null;
        // Currently we do not put other data except family, so it is ok to
        // judge by length that if family was specified
        if (procArgs.length > 0) {
          try {
            HBaseProtos.NameStringPair nsp = HBaseProtos.NameStringPair.parseFrom(procArgs);
            family = nsp.getValue();
          } catch (Exception e) {
            LOG.error("fail to get family by parsing from data", e);
            hasError |= true;
          }
        }
        Subprocedure subprocedure = rsManager.buildSubprocedure(procName, family);
        hasError |= (hasRegion && subprocedure == null || !hasRegion && subprocedure != null);
        return subprocedure;
      }
    });
  }

  @Test
  public void testInvalidSubProcedure()
    throws IOException, NoSuchFieldException, IllegalAccessException, InterruptedException {
    TableName tableName = TableName.valueOf("test_table");
    int count = createAndLoadTable(tableName);
    List<HRegionLocation> regionLocationList = connection.getRegionLocator(tableName).getAllRegionLocations();
    ServerName serverName = regionLocationList.stream().map(v -> v.getServerName()).findAny().get();
    String regionName = regionLocationList.get(0).getRegion().getEncodedName();
    HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
    HRegionServer regionServer0 = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    HRegionServer regionServer1 = TEST_UTIL.getHBaseCluster().getRegionServer(1);
    setRSSnapshotProcManagerMock(regionServer0, regionServer0.getServerName().equals(serverName));
    setRSSnapshotProcManagerMock(regionServer1, regionServer1.getServerName().equals(serverName));
    assertNotEquals(0, region.getMemStoreDataSize());
    admin.flush(tableName);
    assertFalse(hasError);
    assertEquals(0, region.getMemStoreDataSize());
    assertEquals(count, TEST_UTIL.countRows(tableName));
  }
}
