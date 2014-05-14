package org.apache.hadoop.hbase.regionserver;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestFlashBackQuery extends FlashBackQueryTestUtil {
  private static Log LOG = LogFactory.getLog(TestFlashBackQuery.class);
  private static final HashMap<String, HashMap<String, PriorityQueue<KeyValue>>> heapKvs = new HashMap<String, HashMap<String, PriorityQueue<KeyValue>>>();

  private HashMap<Long, Integer> getMap(long effectiveTS, HTable table)
      throws Exception {
    Scan scan = new Scan();
    scan.setMaxVersions();
    scan.setEffectiveTS(effectiveTS);
    HashMap<Long, Integer> tableMap = new HashMap<Long, Integer>();
    for (Result result : table.getScanner(scan)) {
      for (KeyValue kv : result.list()) {
        Integer val = tableMap.get(kv.getTimestamp());
        int ival = (val == null) ? 0 : val;
        tableMap.put(kv.getTimestamp(), ival + 1);
        LOG.info("Got kv : " + kv + " effective : " + effectiveTS);
      }
    }
    return tableMap;
  }

  private void verify(HTable table) throws Exception {
    HashMap<Long, Integer> tableMap = getMap(HConstants.LATEST_TIMESTAMP, table);

    assertEquals(10, (int) tableMap.get(50000L));
    assertEquals(10, (int) tableMap.get(40000L));
    verify1(table);
  }

  private void verify1(HTable table) throws Exception {
    verify1(table, 0);
  }

  private void verify1(HTable table, int deleteVersions) throws Exception {
    int expected3k = 10 - deleteVersions;
    HashMap<Long, Integer> tableMap = getMap(35000, table);

    assertEquals(expected3k, (int) tableMap.get(30000L));
    assertEquals(10, (int) tableMap.get(20000L));
  }

  @Test
  public void testTTL() throws Exception {
    HTable table = setupTable(20, 30, 10);
    byte[] row = new byte[10];
    for (int i = 0; i < 10; i++) {
      random.nextBytes(row);
      Put put = new Put(row);
      put.add(new KeyValue(row, families[0], families[0], 50000, row));
      put.add(new KeyValue(row, families[0], families[0], 40000, row));
      put.add(new KeyValue(row, families[0], families[0], 30000, row));
      put.add(new KeyValue(row, families[0], families[0], 20000, row));
      put.add(new KeyValue(row, families[0], families[0], 10000, row));
      table.put(put);

      Get get = new Get(row);
      get.addColumn(families[0], families[0]);
      get.setEffectiveTS(35000);
      List<KeyValue> kvs = table.get(get).list();
      assertEquals(1, kvs.size());
      assertEquals(kvs.get(0), new KeyValue(row, families[0], families[0],
          30000, row));
    }

    verify(table);
    flushAllRegions();
    verify(table);
    majorCompact(table.getTableName(), table.getTableDescriptor()
        .getColumnFamilies());
    verify(table);
  }

  @Test
  public void testMaxVersions() throws Exception {
    HTable table = setupTable(60, 60, 2);
    byte[] row = new byte[10];
    for (int i = 0; i < 10; i++) {
      random.nextBytes(row);
      Put put = new Put(row);
      put.add(new KeyValue(row, families[0], families[0], 50000, row));
      put.add(new KeyValue(row, families[0], families[0], 40000, row));
      put.add(new KeyValue(row, families[0], families[0], 30000, row));
      put.add(new KeyValue(row, families[0], families[0], 20000, row));
      put.add(new KeyValue(row, families[0], families[0], 10000, row));
      table.put(put);

      Get get = new Get(row);
      get.addColumn(families[0], families[0]);
      get.setEffectiveTS(35000);
      List<KeyValue> kvs = table.get(get).list();
      assertEquals(1, kvs.size());
      assertEquals(kvs.get(0), new KeyValue(row, families[0], families[0],
          30000, row));
    }

    verify(table);
    flushAllRegions();
    verify(table);
    majorCompact(table.getTableName(), table.getTableDescriptor()
        .getColumnFamilies());
    verify(table);
  }

  @Test
  public void testDeletes() throws Exception {
    HTable table = setupTable(20, 30, 2);
    byte[] row = new byte[10];
    int deleteVersions = 0;
    for (int i = 0; i < 10; i++) {
      random.nextBytes(row);
      Put put = new Put(row);
      put.add(new KeyValue(row, families[0], null, 50000, row));
      put.add(new KeyValue(row, families[0], null, 40000, row));
      put.add(new KeyValue(row, families[0], null, 30000, row));
      put.add(new KeyValue(row, families[0], null, 20000, row));
      put.add(new KeyValue(row, families[0], null, 10000, row));
      table.put(put);
      Delete delete = new Delete(row);
      int n = random.nextInt(4);
      if (n == 0) {
        delete.deleteFamily(families[0], 40000);
      } else if (n == 1) {
        delete.deleteRow(40000);
      } else if (n == 2) {
        delete.deleteColumns(families[0], null, 40000);
      } else {
        deleteVersions++;
        delete.deleteColumn(families[0], null, 30000);
      }
      table.delete(delete);
    }
    verify1(table, deleteVersions);
    flushAllRegions();
    verify1(table, deleteVersions);
    majorCompact(table.getTableName(), table.getTableDescriptor()
        .getColumnFamilies());
    verify1(table, deleteVersions);
  }

  private KeyValue processKV(byte[] row, HColumnDescriptor hcd, int start,
      int size, long effectiveTS) {
    boolean isDelete = random.nextBoolean();
    KeyValue kv = getRandomKv(row, hcd, start, size, isDelete);
    if (kv.getTimestamp() <= effectiveTS &&
        kv.getTimestamp() >= effectiveTS - (hcd.getTimeToLive() * 1000)) {
      processHeapKvs(heapKvs, row, hcd, kv);
    }
    return kv;
  }

  private void verifyRandom(long effectiveTS, HTable table) throws Exception {
    Scan scan = new Scan();
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.setEffectiveTS(effectiveTS);
    HashSet<KeyValue> tableSet = new HashSet<KeyValue>();
    for (Result result : table.getScanner(scan)) {
      for (KeyValue kv : result.list()) {
        assertTrue("KV : " + kv + " should not exist", inHeapKvs(kv, heapKvs));
        tableSet.add(kv);
      }
    }

    verifyHeapKvs(heapKvs, tableSet);
  }

  @Test
  public void testRandom() throws Exception {
    int ttl = random.nextInt(CURRENT_TIME/1000);
    int flashBackQueryLimit = random.nextInt(CURRENT_TIME/1000);
    int maxVersions = random.nextInt(MAX_MAXVERSIONS);
    long effectiveTS = CURRENT_TIME
        - random.nextInt(flashBackQueryLimit * 1000 + 1);
    LOG.info("PARAMS : " + ttl + " : " + flashBackQueryLimit + " : "
        + maxVersions + " : " + effectiveTS);
    HTable table = setupTable(ttl, flashBackQueryLimit, maxVersions);
    HColumnDescriptor[] hcds = table.getTableDescriptor().getColumnFamilies();
    byte[] row = new byte[10];
    for (int i = 0; i < 10; i++) {
      random.nextBytes(row);
      Put put = new Put(row);
      int size = CURRENT_TIME / MAX_MAXVERSIONS;
      for (HColumnDescriptor hcd : hcds) {
        for (int versions = 0, start = 0; versions < MAX_MAXVERSIONS; versions++, start += size) {
          put.add(this.processKV(row, hcd, start, size, effectiveTS));
        }
      }
      table.put(put);
    }
    verifyRandom(effectiveTS, table);
    flushAllRegions();
    verifyRandom(effectiveTS, table);
    majorCompact(table.getTableName(), table.getTableDescriptor()
        .getColumnFamilies());
    verifyRandom(effectiveTS, table);
  }
}
