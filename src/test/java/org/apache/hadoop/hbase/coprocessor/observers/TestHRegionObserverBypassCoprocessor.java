package org.apache.hadoop.hbase.coprocessor.observers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.hbase.coprocessor.environments.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHRegionObserverBypassCoprocessor {

  private static HBaseTestingUtility util;
  private static final byte[] tableName = Bytes.toBytes("test");
  private static final byte[] dummy = Bytes.toBytes("dummy");
  private static final byte[] row1 = Bytes.toBytes("r1");
  private static final byte[] row2 = Bytes.toBytes("r2");
  private static final byte[] row3 = Bytes.toBytes("r3");
  private static final byte[] test = Bytes.toBytes("test");

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
    util.createTable(tableName, new byte[][] {dummy, test});
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
    Put p = new Put(row1);
    p.add(test,dummy,dummy);
    p.add(dummy, dummy, dummy);
    puts.add(p);

    p = new Put(row2);
    p.add(dummy, dummy, dummy);
    puts.add(p);

    p = new Put(row3);
    p.add(dummy, dummy, dummy);
    puts.add(p);

    t.put(puts);

    Result r = t.get(new Get(row1));
    Assert.assertTrue(
        "There should be zero results since the put contains \"test\" CF",
        r.isEmpty());

    r = t.get(new Get(row2));
    Assert.assertEquals(1, r.getKvs().size());

    r = t.get(new Get(row3));
    Assert.assertEquals(1, r.getKvs().size());

    t.close();
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
      if (familyMap.containsKey(test)) {
        e.bypass();
        System.out.println("bypassing put: " + put);
      }
    }
  }

}
