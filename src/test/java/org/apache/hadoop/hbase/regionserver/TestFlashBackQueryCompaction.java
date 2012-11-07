/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.util.Comparator;
import java.util.HashSet;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.InjectionEvent;
import org.apache.hadoop.hbase.util.InjectionHandler;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFlashBackQueryCompaction {

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Random random = new Random();
  private static final int CURRENT_TIME = 60000;
  private static final int MAX_MAXVERSIONS = 100;
  private static final HashSet<KeyValue> goodKvs = new HashSet<KeyValue>();
  private static final HashMap<String, HashMap<String, PriorityQueue<KeyValue>>> heapKvs = new HashMap<String, HashMap<String, PriorityQueue<KeyValue>>>();
  private static final HashMap<String, HashMap<String, PriorityQueue<KeyValue>>> heapKvsNormalScan = new HashMap<String, HashMap<String, PriorityQueue<KeyValue>>>();
  private static final Log LOG = LogFactory
      .getLog(TestFlashBackQueryCompaction.class);
  private static final byte[][] families = new byte[][] { "a".getBytes(),
      "b".getBytes(), "c".getBytes(), "d".getBytes(), "e".getBytes(),
      "f".getBytes(), "g".getBytes(), "h".getBytes(), "i".getBytes(),
      "j".getBytes() };

  private static class KVComparator implements Comparator<KeyValue> {

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

  private void processHeapKvs(
      HashMap<String, HashMap<String, PriorityQueue<KeyValue>>> heapKvs,
      byte[] row, HColumnDescriptor hcd, KeyValue kv) {
    HashMap<String, PriorityQueue<KeyValue>> rowMap = heapKvs.get(new String(
        row));
    if (rowMap == null) {
      rowMap = new HashMap<String, PriorityQueue<KeyValue>>();
      heapKvs.put(new String(row), rowMap);
    }
    PriorityQueue<KeyValue> q = rowMap.get(new String(hcd.getName()));
    if (q == null) {
      q = new PriorityQueue<KeyValue>(1, new KVComparator());
      rowMap.put(new String(hcd.getName()), q);
    }
    q.add(kv);
    if (q.size() > hcd.getMaxVersions()) {
      q.poll();
    }
  }

  private KeyValue getRandomKv(byte[] row, HColumnDescriptor hcd, int start,
      int size) {
    byte[] value = new byte[10];
    random.nextBytes(value);
    long ts = start + random.nextInt(size);
    return new KeyValue(row, hcd.getName(), null, ts, value);
  }

  private KeyValue processKV(byte[] row, HColumnDescriptor hcd, int start,
      int size) {
    KeyValue kv = getRandomKv(row, hcd, start, size);
    if (kv.getTimestamp() >= CURRENT_TIME - hcd.getTimeToLive() * 1000) {
      processHeapKvs(heapKvsNormalScan, row, hcd, kv);
    }
    if (kv.getTimestamp() >= CURRENT_TIME - hcd.getFlashBackQueryLimit() * 1000
        - hcd.getTimeToLive() * 1000) {
      if (kv.getTimestamp() > CURRENT_TIME - hcd.getFlashBackQueryLimit()
          * 1000) {
        goodKvs.add(kv);
      } else {
        processHeapKvs(heapKvs, row, hcd, kv);
      }
    }
    return kv;
  }

  private HTable setupTable(int ttl, int flashBackQueryLimit, int maxVersions)
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

  private void flushAllRegions() throws Exception {
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    for (HRegion region : server.getOnlineRegions()) {
      server.flushRegion(region.getRegionName());
    }
  }

  private void loadTable(int ttl, int flashBackQueryLimit, int maxVersions)
      throws Exception {
    HTable table = setupTable(ttl, flashBackQueryLimit, maxVersions);
    HColumnDescriptor[] hcds = table.getTableDescriptor().getColumnFamilies();
    byte[] tableName = table.getTableName();
    byte[] row = new byte[10];
    for (int i = 0; i < 10; i++) {
      random.nextBytes(row);
      Put put = new Put(row);
      int size = CURRENT_TIME / MAX_MAXVERSIONS;
      for (HColumnDescriptor hcd : hcds) {
        for (int versions = 0, start = 0; versions < MAX_MAXVERSIONS; versions++, start += size) {
          put.add(this.processKV(row, hcd, start, size));
        }
      }
      table.put(put);
    }

    // flush all regions synchronously.
    flushAllRegions();

    // major compact everything.
    majorCompact(tableName, hcds);

    setStoreProps(tableName, hcds, false);

    // verify all kvs in all store files.
    Scan scan = new Scan();
    scan.setMaxVersions(Integer.MAX_VALUE);
    HashSet<KeyValue> tableSet = new HashSet<KeyValue>();
    for (Result result : table.getScanner(scan)) {
      for (KeyValue kv : result.list()) {
        assertTrue("KV : " + kv + " should not exist", goodKvs.contains(kv)
            || inHeapKvs(kv, heapKvs));
        tableSet.add(kv);
      }
    }

    for (KeyValue kv : goodKvs) {
      assertTrue("KV in goodKvs: " + kv + " does not exist in table",
          tableSet.contains(kv));
    }

    verifyHeapKvs(heapKvs, tableSet);

    setStoreProps(tableName, hcds, true);
    LOG.info("Scanning normal");
    scan = new Scan();
    scan.setMaxVersions(Integer.MAX_VALUE);
    tableSet = new HashSet<KeyValue>();
    for (Result result : table.getScanner(scan)) {
      for (KeyValue kv : result.list()) {
        LOG.info("Last SCAN KV : " + kv);
        assertTrue("KV : " + kv + " should not exist",
            inHeapKvs(kv, heapKvsNormalScan));
        tableSet.add(kv);
      }
    }

    verifyHeapKvs(heapKvsNormalScan, tableSet);
  }

  private void majorCompact(byte[] tableName, HColumnDescriptor[] hcds)
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

  private void setStoreProps(byte[] tableName, HColumnDescriptor[] hcds,
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

  private void verifyHeapKvs(
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

  private boolean inHeapKvs(KeyValue kv,
      HashMap<String, HashMap<String, PriorityQueue<KeyValue>>> heapKvs) {
    HashMap<String, PriorityQueue<KeyValue>> rowMap = heapKvs.get(new String(kv
        .getRow()));
    if (rowMap == null)
      return false;
    return rowMap.get(new String(kv.getFamily())).contains(kv);
  }

  @Test
  public void testRandom() throws Exception {
    loadTable(random.nextInt(CURRENT_TIME) / 1000,
        random.nextInt(CURRENT_TIME) / 1000, random.nextInt(MAX_MAXVERSIONS));
  }

  private class CompactionHookHandler extends InjectionHandler {
    protected void _processEvent(InjectionEvent event, Object... args) {
      if (InjectionEvent.STORE_AFTER_APPEND_KV == event) {
        KeyValue kv = (KeyValue) args[0];
        if (Type.codeToType(kv.getType()) == Type.Put) {
          compactionSet.add((KeyValue) args[0]);
        }
      }
    }
  }

  private static HashSet<KeyValue> compactionSet = new HashSet<KeyValue>();

  private KeyValue loadTableAndDelete(boolean family) throws Exception {
    HTable table = setupTable(30, 20, 100);
    byte[] row = new byte[10];
    Put put = new Put(row);
    KeyValue kv1 = new KeyValue(row, families[0], null, 55000, row);
    KeyValue kv2 = new KeyValue(row, families[0], null, 35000, row);
    put.add(kv1);
    put.add(kv2);
    table.put(put);

    Delete delete = new Delete(row);
    if (!family) {
      delete.deleteRow(57000);
    } else {
      delete.deleteFamily(families[0], 57000);
    }
    table.delete(delete);

    flushAllRegions();

    compactionSet.clear();
    majorCompact(table.getTableName(), table.getTableDescriptor()
        .getColumnFamilies());

    assertEquals(2, compactionSet.size());

    delete = new Delete(row);
    if (!family) {
      delete.deleteRow(37000);
    } else {
      delete.deleteFamily(families[0], 37000);
    }
    table.delete(delete);

    flushAllRegions();

    compactionSet.clear();
    majorCompact(table.getTableName(), table.getTableDescriptor()
        .getColumnFamilies());

    return kv1;
  }

  @Test
  public void testDeleteRow() throws Exception {
    InjectionHandler.set(new CompactionHookHandler());
    try {
      KeyValue kv1 = loadTableAndDelete(false);
      assertEquals(1, compactionSet.size());
      assertTrue(compactionSet.contains(kv1));
    } finally {
      InjectionHandler.clear();
    }
  }

  @Test
  public void testDeleteFamily() throws Exception {
    InjectionHandler.set(new CompactionHookHandler());
    try {
      KeyValue kv1 = loadTableAndDelete(true);
      assertEquals(1, compactionSet.size());
      assertTrue(compactionSet.contains(kv1));
    } finally {
      InjectionHandler.clear();
    }
  }

  @Test
  public void testCornerCaseTTL() throws Exception {
    HTable table = setupTable(30, 20, 100);
    byte[] row = new byte[10];
    Put put = new Put(row);
    KeyValue kv1 = new KeyValue(row, families[0], null, 10000, row);
    KeyValue kv2 = new KeyValue(row, families[0], null, 9999, row);
    put.add(kv1);
    put.add(kv2);
    table.put(put);

    flushAllRegions();

    majorCompact(table.getTableName(), table.getTableDescriptor()
        .getColumnFamilies());

    // Get all kvs.
    setStoreProps(table.getTableName(), table.getTableDescriptor()
        .getColumnFamilies(), false);

    Scan scan = new Scan(row);
    scan.setMaxVersions();
    HashSet<KeyValue> kvSet = new HashSet<KeyValue>();
    for (Result result : table.getScanner(scan)) {
      kvSet.addAll(result.list());
    }
    assertTrue(kvSet.contains(kv1));
    assertFalse(kvSet.contains(kv2));
  }

  @Test
  public void testCornerCaseMaxVersions() throws Exception {
    HTable table = setupTable(30, 20, 2);
    byte[] row = new byte[10];
    Put put = new Put(row);
    KeyValue kv1 = new KeyValue(row, families[0], null, 50000, row);
    KeyValue kv2 = new KeyValue(row, families[0], null, 40000, row);
    KeyValue kv3 = new KeyValue(row, families[0], null, 15000, row);
    KeyValue kv4 = new KeyValue(row, families[0], null, 10000, row);
    put.add(kv1);
    put.add(kv2);
    put.add(kv3);
    put.add(kv4);
    table.put(put);

    flushAllRegions();

    majorCompact(table.getTableName(), table.getTableDescriptor()
        .getColumnFamilies());

    // Get all kvs.
    setStoreProps(table.getTableName(), table.getTableDescriptor()
        .getColumnFamilies(), false);

    Scan scan = new Scan(row);
    scan.setMaxVersions();
    HashSet<KeyValue> kvSet = new HashSet<KeyValue>();
    for (Result result : table.getScanner(scan)) {
      kvSet.addAll(result.list());
    }
    assertTrue(kvSet.contains(kv1));
    assertTrue(kvSet.contains(kv2));
    assertTrue(kvSet.contains(kv3));
    assertFalse(kvSet.contains(kv4));
  }
}
