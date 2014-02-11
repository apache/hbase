/*
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCompoundRowPrefixFilter {

  private byte[][] PREFIXES = new byte[][] {
      Bytes.toBytes("com.abc.news"),
      Bytes.toBytes("com.google.news"),
      Bytes.toBytes("com.fb.apps")
  };

  private byte[][] keys = new byte[][] {
      Bytes.toBytes("com.abc.ads"),
      Bytes.toBytes("com.abc.news.india"),
      Bytes.toBytes("com.abc.news.us"),
      Bytes.toBytes("com.fb.ads"),
      Bytes.toBytes("com.fb.ads.feed"),
      Bytes.toBytes("com.fb.apps.bitcoins"),
      Bytes.toBytes("com.google.news.india"),
      Bytes.toBytes("com.zee.news.us")
  };
  private byte[] tableName = Bytes.toBytes("testCompoundRowPrefixFilter");
  private byte[] familyName = Bytes.toBytes("cf");
  private HRegion region = null;
  private Configuration conf = null;
  private Path testDir = null;

  private static HBaseTestingUtility util = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    util.startMiniCluster();
  }

  public void setup() throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(familyName));
    HRegionInfo info = new HRegionInfo(htd, null, null, false);
    this.testDir = util.getTestDir(Bytes.toString(tableName));
    this.conf = util.getConfiguration();
    this.region = HRegion.createHRegion(info, testDir, this.conf);
    this.region.flushcache();
    loadData(this.familyName);
  }

  private List<Put> generatePuts(byte[] familyName) {
    List<Put> lst = new ArrayList<Put>();
    for (byte[] key : keys) {
      Put p = new Put(key);
      p.add(familyName, null, key);
      lst.add(p);
    }
    return lst;
  }

  private void loadData(byte[] familyName) throws IOException {
    for (Put p : generatePuts(familyName)) {
      region.put(p);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testSerDe() throws IOException {
    CompoundRowPrefixFilter filter = new CompoundRowPrefixFilter.Builder()
      .addRowPrefix(PREFIXES[0])
      .addRowPrefix(PREFIXES[1])
      .addRowPrefix(PREFIXES[2])
      .create();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    filter.write(dos);
    dos.close();
    byte[] byteArray = baos.toByteArray();
    ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
    DataInputStream dis = new DataInputStream(bais);
    CompoundRowPrefixFilter deserializedFilter = new CompoundRowPrefixFilter();
    deserializedFilter.readFields(dis);
    assertTrue(filter.equals(deserializedFilter));
  }

  private Filter createFilter() {
    Filter filter = new CompoundRowPrefixFilter.Builder()
    .addRowPrefix(PREFIXES[0])
    .addRowPrefix(PREFIXES[1])
    .addRowPrefix(PREFIXES[2])
    .create();
    return filter;
  }

  @Test
  public void testFilterBehaviour () {
    Filter filter = createFilter();

    assertTrue(!filter.filterRowKey(keys[0], 0, keys[0].length));
    assertTrue(!filter.filterAllRemaining());
    assertTrue(filter.filterKeyValue(KeyValue.createFirstOnRow(keys[0]))
        == ReturnCode.SEEK_NEXT_USING_HINT);
    assertTrue(Bytes.compareTo(
        filter.getNextKeyHint(KeyValue.createFirstOnRow(keys[0])).getRow(),
        PREFIXES[0]) == 0);
    assertTrue(filter.filterKeyValue(KeyValue.createFirstOnRow(keys[1]))
        == ReturnCode.INCLUDE);
    assertTrue(filter.filterKeyValue(KeyValue.createFirstOnRow(keys[2]))
        == ReturnCode.INCLUDE);
    assertTrue(!filter.filterAllRemaining());
    assertTrue(filter.filterKeyValue(KeyValue.createFirstOnRow(keys[3]), null)
        == ReturnCode.SEEK_NEXT_USING_HINT);
    assertTrue(Bytes.compareTo(filter.getNextKeyHint(
        KeyValue.createFirstOnRow(keys[3])).getRow(), PREFIXES[2]) == 0);
    assertTrue(filter.filterKeyValue(KeyValue.createFirstOnRow(keys[5]), null)
        == ReturnCode.INCLUDE);
    assertTrue(filter.filterKeyValue(KeyValue.createFirstOnRow(keys[6]), null)
        == ReturnCode.INCLUDE);
    assertTrue(Bytes.compareTo(filter.getNextKeyHint(
        KeyValue.createFirstOnRow(keys[6])).getRow(), PREFIXES[1]) == 0);
    assertTrue(filter.filterKeyValue(KeyValue.createFirstOnRow(keys[7]), null)
        == ReturnCode.SKIP);
    assertTrue(filter.filterAllRemaining());
  }

  private Scan createScan() {
    Scan s = new Scan();
    s.setStartRow(Bytes.toBytes(""));
    s.setStopRow(Bytes.toBytes("zzz"));
    Filter f = createFilter();
    s.setFilter(f);
    return s;
  }

  @Test
  public void testScannerBehavior() throws IOException {
    setup();
    InternalScanner scanner = this.region.getScanner(createScan());
    verifyScan(scanner);
  }

  private void verifyScan(InternalScanner scanner) throws IOException {
    List<KeyValue> results = new ArrayList<KeyValue>();

    scanner.next(results);
    assertTrue(results.size() == 1);
    assertTrue(Bytes.compareTo(results.get(0).getRow(), keys[1]) == 0);
    results.clear();

    scanner.next(results);
    assertTrue(results.size() == 1);
    assertTrue(Bytes.compareTo(results.get(0).getRow(), keys[2]) == 0);
    results.clear();

    scanner.next(results);
    assertTrue(results.size() == 1);
    assertTrue(Bytes.compareTo(results.get(0).getRow(), keys[5]) == 0);
    results.clear();

    scanner.next(results);
    assertTrue(results.size() == 1);
    assertTrue(Bytes.compareTo(results.get(0).getRow(), keys[6]) == 0);
    results.clear();
  }

  private HTable setupForE2E(String tableName, String family) throws IOException {
    HTable table = util.createTable(Bytes.toBytes(tableName),
        Bytes.toBytes(family));
    table.put(generatePuts(Bytes.toBytes(family)));
    table.flushCommits();
    return table;
  }

  @Test
  public void testFilterE2E() throws IOException {
    HTable table = setupForE2E("testFilterE2E", "cf");
    ResultScanner scanner = table.getScanner(createScan());

    Result r = scanner.next();
    assertTrue(Bytes.compareTo(r.getRow(), keys[1]) == 0);

    r = scanner.next();
    assertTrue(Bytes.compareTo(r.getRow(), keys[2]) == 0);

    r = scanner.next();
    assertTrue(Bytes.compareTo(r.getRow(), keys[5]) == 0);

    r = scanner.next();
    assertTrue(Bytes.compareTo(r.getRow(), keys[6]) == 0);
  }
}
