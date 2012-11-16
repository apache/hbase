package org.apache.hadoop.hbase.regionserver;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;

public class FlashBackQueryTestUtil {

  private static final Log LOG = LogFactory
      .getLog(FlashBackQueryTestUtil.class);
  protected static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static Random random = new Random();
  protected static final int CURRENT_TIME = 60000;
  protected static final int MAX_MAXVERSIONS = 100;
  protected static final byte[][] families = new byte[][] { "a".getBytes(),
      "b".getBytes(), "c".getBytes(), "d".getBytes(), "e".getBytes(),
      "f".getBytes(), "g".getBytes(), "h".getBytes(), "i".getBytes(),
      "j".getBytes() };

  protected static class KVComparator implements Comparator<KeyValue> {

    @Override
    public int compare(KeyValue kv1, KeyValue kv2) {
      return (kv1.getTimestamp() < kv2.getTimestamp() ? -1 : (kv1
          .getTimestamp() > kv2.getTimestamp() ? 1 : 0));
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    ManualEnvironmentEdge mock = new ManualEnvironmentEdge();
    mock.setValue(CURRENT_TIME);
    EnvironmentEdgeManagerTestHelper.injectEdge(mock);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    EnvironmentEdgeManagerTestHelper.reset();
  }

  protected HTable setupTable(int ttl, int flashBackQueryLimit, int maxVersions)
      throws Exception {
    LOG.info("maxVersions : " + maxVersions);
    LOG.info("TTL : " + ttl);
    LOG.info("FBQ: " + flashBackQueryLimit);
    byte[] tableName = ("loadTable" + random.nextInt()).getBytes();
    HColumnDescriptor[] hcds = new HColumnDescriptor[random
        .nextInt(families.length - 1) + 1];
    for (int i = 0; i < hcds.length; i++) {
      hcds[i] = new HColumnDescriptor(families[i]);
      hcds[i].setTimeToLive(ttl);
      hcds[i].setFlashBackQueryLimit(flashBackQueryLimit);
      hcds[i].setMaxVersions(maxVersions);
    }
    return TEST_UTIL.createTable(tableName, hcds);
  }

  protected void flushAllRegions() throws Exception {
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    for (HRegion region : server.getOnlineRegions()) {
      server.flushRegion(region.getRegionName());
    }
  }

  protected KeyValue getRandomKv(byte[] row, HColumnDescriptor hcd, int start,
      int size, boolean isDelete) {
    byte[] value = new byte[10];
    random.nextBytes(value);
    long ts = start + random.nextInt(size);
    if (isDelete) {
      Type type = null;
      if (random.nextBoolean()) {
        type = Type.DeleteColumn;
      } else {
        type = Type.DeleteFamily;
      }
      return new KeyValue(row, hcd.getName(), null, ts, type,
          value);
    }
    return new KeyValue(row, hcd.getName(), null, ts, value);
  }

  protected void majorCompact(byte[] tableName, HColumnDescriptor[] hcds)
      throws Exception {
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    for (HRegion region : server.getOnlineRegions()) {
      if (!new String(region.getTableDesc().getName()).equals(new String(
          tableName))) {
        LOG.info("Skipping since name is : "
            + new String(region.getTableDesc().getName()));
        continue;
      }
      for (HColumnDescriptor hcd : hcds) {
        Store store = region.getStore(hcd.getName());
        store.compactRecentForTesting(-1);
      }
    }
  }

  protected void processHeapKvs(
      HashMap<String, HashMap<String, PriorityQueue<KeyValue>>> heapKvs,
      byte[] row, HColumnDescriptor hcd, KeyValue kv) {
    HashMap<String, PriorityQueue<KeyValue>> rowMap = heapKvs.get(new String(
        row));
    if (rowMap == null) {
      rowMap = new HashMap<String, PriorityQueue<KeyValue>>();
      heapKvs.put(new String(row), rowMap);
    }
    PriorityQueue<KeyValue> q = rowMap.get(new String(hcd.getName()));
    if (kv.isDelete()) {
      if (q != null) {
        // Timestamps appear in increasing order.
        LOG.info("Clearing out at : " + kv);
        q.clear();
      }
      return;
    }
    if (q == null) {
      q = new PriorityQueue<KeyValue>(1, new KVComparator());
      rowMap.put(new String(hcd.getName()), q);
    }
    q.add(kv);
    LOG.info("Added kv : " + kv);
    if (q.size() > hcd.getMaxVersions()) {
      q.poll();
    }

  }

  protected void setStoreProps(byte[] tableName, HColumnDescriptor[] hcds,
      boolean def) {
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    for (HRegion region : server.getOnlineRegions()) {
      if (!new String(region.getTableDesc().getName()).equals(new String(
          tableName))) {
        LOG.info("Skipping since name is : "
            + new String(region.getTableDesc().getName()));
        continue;
      }
      for (HColumnDescriptor hcd : hcds) {
        Store store = region.getStore(hcd.getName());
        // Reset back to original values.
        if (def) {
          store.ttl = hcd.getTimeToLive() * 1000;
          store.getFamily().setMaxVersions(hcd.getMaxVersions());
        } else {
          store.ttl = HConstants.FOREVER;
          store.getFamily().setMaxVersions(Integer.MAX_VALUE);
        }
      }
    }
  }

  protected boolean inHeapKvs(KeyValue kv,
      HashMap<String, HashMap<String, PriorityQueue<KeyValue>>> heapKvs) {
    HashMap<String, PriorityQueue<KeyValue>> rowMap = heapKvs.get(new String(kv
        .getRow()));
    if (rowMap == null)
      return false;
    return rowMap.get(new String(kv.getFamily())).contains(kv);
  }

  protected void verifyHeapKvs(
      HashMap<String, HashMap<String, PriorityQueue<KeyValue>>> heapKvs,
      HashSet<KeyValue> tableSet) {
    for (HashMap<String, PriorityQueue<KeyValue>> rowMap : heapKvs.values()) {
      for (PriorityQueue<KeyValue> q : rowMap.values()) {
        for (KeyValue kv : q) {
          assertTrue("KV in heapKvs: " + kv + " does not exist in table",
              tableSet.contains(kv));
        }
      }
    }
  }

}
