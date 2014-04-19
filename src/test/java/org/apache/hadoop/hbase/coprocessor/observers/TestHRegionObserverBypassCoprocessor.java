package org.apache.hadoop.hbase.coprocessor.observers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.environments.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Testing basic coprocessor behavior
 *
 */
public class TestHRegionObserverBypassCoprocessor {

  private static HBaseTestingUtility util;
  private static final byte[] tableName = Bytes.toBytes("test");
  private static final int MAX_ROWS = 6;
  private static final List<byte[]> rows = new ArrayList<>();
  static {
    for (int i=0; i< MAX_ROWS; i++){
      rows.add(Bytes.toBytes("row" + i));
    }
  }
  private static final byte[] DUMMY = Bytes.toBytes("dummy");
  private static final byte[] TEST = Bytes.toBytes("test");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        TestCoprocessor.class.getName());
    util = new HBaseTestingUtility(conf);
    util.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    HBaseAdmin admin = util.getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }
    util.createTable(tableName, new byte[][] {DUMMY, TEST});
  }

  /**
   * Make 3 puts, one of them will have test column family - so it should be
   * skipped during insertion
   *
   * @throws Exception
   */
  @Test
  public void testSimple() throws Exception {
    HTable t = new HTable(util.getConfiguration(), tableName);
    List<Put> puts = new ArrayList<Put>();
    Put p = new Put(rows.get(0));
    p.add(TEST,DUMMY,DUMMY);
    p.add(DUMMY, DUMMY, DUMMY);
    puts.add(p);

    p = new Put(rows.get(1));
    p.add(DUMMY, DUMMY, DUMMY);
    puts.add(p);

    p = new Put(rows.get(2));
    p.add(DUMMY, DUMMY, DUMMY);
    puts.add(p);

    t.put(puts);

    Result r = t.get(new Get(rows.get(0)));
    Assert.assertTrue(
        "There should be zero results since the put contains \"test\" CF",
        r.isEmpty());

    r = t.get(new Get(rows.get(1)));
    Assert.assertEquals(1, r.getKvs().size());

    r = t.get(new Get(rows.get(2)));
    Assert.assertEquals(1, r.getKvs().size());
    t.close();
  }

  /**
   * Scenario this test case is testing
   * <ul>
   * <li>Check whether the first coprocessor is running</li>
   * <li>Change the configuration such that a second coprocessor is added and
   * check whether both of them are in the list of loaded coprocessors in each
   * region</li>
   * <li> test wheteher both of them are functioning right </li>
   * <li> remove both coprocessors and confirm regions are not running them</li>
   * <li> remove there is no action taken by any coprocessor since they are unloaded</li>
   * <li> add back the first coprocessor and confirm regions have it and it is functioning fine </li>
   * </ul>
   * @throws Exception
   */
  @Test
  public void testDynamicLoadingOfCoprocessorsInRegionCoprocessorHost() throws Exception {
    HTable table = new HTable(util.getConfiguration(), tableName);
    Set<String> allCoprocessors = RegionCoprocessorHost
        .getEverLoadedCoprocessors();
    Assert.assertEquals("There should be only one coprocessor everloaded", 1, allCoprocessors.size());
    Assert
        .assertEquals(
            "Expected loaded coprocessor is different from one which is currently loaded",
            TestCoprocessor.class.getName(), allCoprocessors.toArray()[0]);
    Configuration conf = util.getConfiguration();
    Assert.assertEquals(TestCoprocessor.class.getName(),
        conf.getStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY)[0]);
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        TestCoprocessor2.class.getName(), TestCoprocessor.class.getName());
    System.out.println("conf strings: "
        + Arrays.toString(conf
            .getStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY)));

    // invoke online configuration change
    HRegionServer.configurationManager.notifyAllObservers(conf);

    // check if the coprocessor is in list of everloaded coprocessors
    Set<String> expectedCoprocessors = new HashSet<>(2);
    expectedCoprocessors.add(TestCoprocessor.class.getName());
    expectedCoprocessors.add(TestCoprocessor2.class.getName());
    Assert.assertEquals("Mismatch in expected loaded coprocessors", expectedCoprocessors,
        RegionCoprocessorHost.getEverLoadedCoprocessors());

    // check if both new and old coprocessors are in the list of current
    // coprocessors in each region
    List<HRegion> regions = util.getMiniHBaseCluster().getRegions(tableName);
    Set<String> expectedCoprocessorSimpleName = new HashSet<>();
    expectedCoprocessorSimpleName.add(TestCoprocessor.class.getSimpleName());
    expectedCoprocessorSimpleName.add(TestCoprocessor2.class.getSimpleName());
    for (HRegion region : regions) {
      Assert.assertEquals("Mismatch in expected loaded coprocessors on region"
          + region.getRegionNameAsString(), expectedCoprocessorSimpleName,
          region.getCoprocessorHost().getCoprocessors());
    }

    //now do some puts and check if both of them are working (none of the puts should go in)
    createPutAndVerifyObserverCorrectness(table, rows.get(0), rows.get(1), true, true);

    //now remove them both
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY, "");
    System.out.println("conf strings: "
        + Arrays.toString(conf
            .getStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY)));

    // invoke online configuration change
    HRegionServer.configurationManager.notifyAllObservers(conf);

    // check if both of them are still in list of everloaded coprocessors
    Assert.assertEquals("Mismatch in expected loaded coprocessors", expectedCoprocessors,
        RegionCoprocessorHost.getEverLoadedCoprocessors());

    // none of the coprocessors should be loaded in the regions
    for (HRegion region : regions) {
      System.out.println(region.getCoprocessorHost().getCoprocessors());
      Assert.assertTrue("Region should not contain any coprocessors", region
          .getCoprocessorHost().getCoprocessors().isEmpty());
    }

    //now do some puts and check if both of them are working (all puts should go in)
    createPutAndVerifyObserverCorrectness(table, rows.get(2), rows.get(3), false, false);

    //add back the first one
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY, TestCoprocessor.class.getName());
    System.out.println("conf strings: "
        + Arrays.toString(conf
            .getStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY)));

    // invoke online configuration change
    HRegionServer.configurationManager.notifyAllObservers(conf);

    //confirm it is the only one on the regions
    expectedCoprocessorSimpleName.remove(TestCoprocessor2.class.getSimpleName());
    for (HRegion region : regions) {
      Assert.assertEquals("Mismatch in expected loaded coprocessors on region",
          expectedCoprocessorSimpleName, region.getCoprocessorHost()
              .getCoprocessors());
    }

    // check if both of them are still in list of everloaded coprocessors (this should not change!)
    Assert.assertEquals("Mismatch in expected loaded coprocessors", expectedCoprocessors,
        RegionCoprocessorHost.getEverLoadedCoprocessors());

    //now do some puts and check if only the first one is working
    createPutAndVerifyObserverCorrectness(table, rows.get(4), rows.get(5), true, false);
  }

  /**
   * Create two puts one with cf dummy and one with cf test and make sure
   * coprocessors are functioning fine when we do put
   *
   * @param table
   *          - Htable instance
   * @param testOn
   *          - whether {@link TestCoprocessor} is enabled
   * @param dummyOn
   *          - whether {@link TestCoprocessor2} is enabled
   * @throws Exception
   */
  public void createPutAndVerifyObserverCorrectness(HTable table, byte[] row1, byte[] row2,
      boolean testOn, boolean dummyOn) throws Exception {
    List<Put> puts = new ArrayList<>();
    Put p = new Put(row1);
    p.add(TEST, DUMMY, DUMMY);
    puts.add(p);

    p = new Put(row2);
    p.add(DUMMY, DUMMY, DUMMY);
    puts.add(p);
    table.put(puts);
    for (Put put : puts) {
      Result r = table.get(new Get(put.getRow()));
      List<KeyValue> kvs = put.getFamilyMap().get(TEST);

      // both puts will have one col family each. This is to check if the put
      // has column family called "test"
      boolean hasFamilyTest = (kvs!= null && !kvs.isEmpty());
      if (testOn && hasFamilyTest) {
        Assert.assertTrue(
            "There should be zero results since the put contains \"test\" CF",
            r.isEmpty());
      } else if (!testOn &&  hasFamilyTest){
        Assert.assertEquals("There should be one result", 1, r.getKvs().size());
      }
      if (dummyOn && !hasFamilyTest) {
        Assert.assertTrue(
            "There should be zero results since the put contains \"dummy\" CF",
            r.isEmpty());
      } else if (!dummyOn && !hasFamilyTest){
        Assert.assertEquals("There should be one result", 1, r.getKvs().size());
      }
    }
  }

  /**
   * Dummy coprocessor which skips put containing "test" as a column family
   *
   */
  public static class TestCoprocessor extends BaseRegionObserver {
    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Put put, final WALEdit edit, final boolean durability)
        throws IOException {
      Map<byte[], List<KeyValue>> familyMap = put.getFamilyMap();
      if (familyMap.containsKey(TEST)) {
        e.bypass();
        System.out.println("bypassing put: " + put);
      }
    }
  }

  /**
   * Dummy coprocessor which skips put containing "dummy" as a column family
   *
   */
  public static class TestCoprocessor2 extends BaseRegionObserver {
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c,
        Put put, WALEdit edit, boolean writeToWAL) throws IOException {
      Map<byte[], List<KeyValue>> familyMap = put.getFamilyMap();
      if (familyMap.containsKey(DUMMY)) {
        c.bypass();
        System.out.println("bypassing put: " + put);
      }
    }
  }

}
