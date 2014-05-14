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

import java.util.Arrays;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.InjectionEvent;
import org.apache.hadoop.hbase.util.InjectionHandler;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestFlashBackQueryCompaction extends FlashBackQueryTestUtil {

  private static final HashSet<KeyValue> goodKvs = new HashSet<KeyValue>();
  private static final HashMap<String, HashMap<String, PriorityQueue<KeyValue>>> heapKvs = new HashMap<String, HashMap<String, PriorityQueue<KeyValue>>>();
  private static final HashMap<String, HashMap<String, PriorityQueue<KeyValue>>> heapKvsNormalScan = new HashMap<String, HashMap<String, PriorityQueue<KeyValue>>>();
  private static final Log LOG = LogFactory
      .getLog(TestFlashBackQueryCompaction.class);

  private KeyValue processKV(byte[] row, HColumnDescriptor hcd, int start,
      int size) {
    KeyValue kv = getRandomKv(row, hcd, start, size, false);
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

  @Test
  public void testRandom() throws Exception {
    loadTable(random.nextInt(CURRENT_TIME) / 1000,
        random.nextInt(CURRENT_TIME) / 1000, random.nextInt(MAX_MAXVERSIONS));
  }

  private static HashSet<KeyValue> compactionSet = new HashSet<KeyValue>();

  private void scanTable(HTable table, long effectiveTS) throws Exception {
    compactionSet.clear();
    Scan s = new Scan();
    s.setEffectiveTS(55000);
    s.setMaxVersions();
    for (Result result : table.getScanner(s)) {
      compactionSet.addAll(result.list());
    }
  }

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

    majorCompact(table.getTableName(), table.getTableDescriptor()
        .getColumnFamilies());
    scanTable(table, 55000);

    assertEquals(2, compactionSet.size());

    delete = new Delete(row);
    if (!family) {
      delete.deleteRow(37000);
    } else {
      delete.deleteFamily(families[0], 37000);
    }
    table.delete(delete);

    flushAllRegions();

    majorCompact(table.getTableName(), table.getTableDescriptor()
        .getColumnFamilies());
    scanTable(table, 55000);

    return kv1;
  }

  @Test
  public void testDeleteRow() throws Exception {
    KeyValue kv1 = loadTableAndDelete(false);
    assertEquals(1, compactionSet.size());
    assertTrue(compactionSet.contains(kv1));
  }

  @Test
  public void testDeleteFamily() throws Exception {
    KeyValue kv1 = loadTableAndDelete(true);
    assertEquals(1, compactionSet.size());
    assertTrue(compactionSet.contains(kv1));
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
