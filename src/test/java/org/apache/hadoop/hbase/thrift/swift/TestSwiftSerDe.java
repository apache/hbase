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

package org.apache.hadoop.hbase.thrift.swift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TRowMutations;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.TFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram.Bucket;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram.HFileStat;
import org.apache.hadoop.hbase.master.AssignmentPlan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Test;

import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.ThriftCodecManager;

public class TestSwiftSerDe {

  byte[] family1 = Bytes.toBytes("family1");
  byte[] family2 = Bytes.toBytes("family2");
  byte[] qual = Bytes.toBytes("qual");
  byte[] row = Bytes.toBytes("rowkey");

  @Test
  public void testTRowMutationsSwiftSerDe() throws Exception {
    ThriftCodec<TRowMutations> codec = new ThriftCodecManager()
        .getCodec(TRowMutations.class);
    TMemoryBuffer transport = new TMemoryBuffer(1000 * 1024);
    TCompactProtocol protocol = new TCompactProtocol(transport);

    Put p1 = new Put(row);
    p1.add(family1, qual, family1);
    Put p2 = new Put(row);
    p2.add(family2, qual, family2);
    Delete d = new Delete(row);
    d.deleteColumn(family1, qual);
    TRowMutations mutations = (new TRowMutations.Builder(row))
        .addPut(p1).addPut(p2).addDelete(d).create();

    codec.write(mutations, protocol);
    TRowMutations copy = codec.read(protocol);
    assertTrue(Bytes.BYTES_COMPARATOR.compare(mutations.getRow(), copy.getRow()) == 0);
    assertEquals(mutations.getPuts().size(), copy.getPuts().size());

    /**
     * 3(instead of 2) Because we insert no-ops in the places
     * corresponding to the deletes
     */
    assertEquals(3, copy.getPuts().size());
    for (int i = 0; i<2; i++) {
      mutations.getPuts().get(i).equals(copy.getPuts().get(i));
    }
    assertEquals(mutations.getDeletes().size(), copy.getDeletes().size());
    assertEquals(3, copy.getDeletes().size());
    mutations.getDeletes().get(0).equals(copy.getDeletes().get(0));
  }

  /**
   * Tests the scan serialization and deserailization with the Scan Builder
   * This one uses the addColumn to construct the scan object.
   * @throws Exception
   */
  @Test
  public void testScanSerDe() throws Exception {

    Scan.Builder b = new Scan.Builder();
    Filter filter = new WhileMatchFilter(
        new PrefixFilter(Bytes.toBytes("poiuy")));
    Scan s = b.addColumn(family1, qual)
      .setBatch(3425)
      .setCacheBlocks(true)
      .setCaching(8647)
      .setEffectiveTS(87434)
      .setMaxResultsPerColumnFamily(7624)
      .setMaxVersions(34)
      .setRowOffsetPerColumnFamily(186434)
      .setServerPrefetching(true)
      .setStartRow(Bytes.toBytes("hfy"))
      .setStopRow(Bytes.toBytes("ijk"))
      .setTimeRange(3455, 8765)
      .setFilter(filter)
      .create();
    byte[] data = Bytes.writeThriftBytes(s, Scan.class);
    Scan copy = Bytes.readThriftBytes(data, Scan.class);
    assertEquals(s, copy);
  }

  /**
   * Constructs the Scan object using addFamily and without the builder
   * @throws Exception
   */
  @Test
  public void testScanSerDe2() throws Exception {
    Scan s = new Scan();
    s.addFamily(family1);
    s.setBatch(3425);
    s.setCacheBlocks(true);
    s.setCaching(8647);
    s.setEffectiveTS(87434);
    s.setMaxResultsPerColumnFamily(7624);
    s.setMaxVersions(34);
    s.setRowOffsetPerColumnFamily(186434);
    s.setServerPrefetching(true);
    s.setStartRow(Bytes.toBytes("hfy"));
    s.setStopRow(Bytes.toBytes("ijk"));
    s.setTimeRange(3455, 8765);
    byte[] data = Bytes.writeThriftBytes(s, Scan.class);
    Scan copy = Bytes.readThriftBytes(data, Scan.class);
    assertEquals(s, copy);
  }

  @Test
  public void testFilterSerDe() throws Exception {
    FilterList f = new FilterList(Operator.MUST_PASS_ALL);
    byte[] randomPrefix = Bytes.toBytes("randomPrefix1");
    PrefixFilter f1 = new PrefixFilter(randomPrefix);
    f.addFilter(f1);
    PageFilter f2 = new PageFilter(1024);
    f.addFilter(f2);
    KeyOnlyFilter f3 = new KeyOnlyFilter(false);
    f.addFilter(f3);
    TFilter tf = TFilter.getTFilter(f);
    byte[] data = Bytes.writeThriftBytes(tf, TFilter.class);
    TFilter copy = Bytes.readThriftBytes(data, TFilter.class);
    FilterList flCopy = (FilterList)copy.getFilter();
    assertEquals(flCopy.getFilters().size(), 3);
    assertTrue(flCopy.getFilters().get(0) instanceof PrefixFilter);
    byte[] expectedRandomPrefix = ((PrefixFilter)flCopy.getFilters().get(0)).getPrefix();
    assertTrue(Bytes.equals(expectedRandomPrefix, randomPrefix));
    assertTrue(flCopy.getFilters().get(1) instanceof PageFilter);
    assertEquals(((PageFilter)flCopy.getFilters().get(1)).getPageSize(), 1024);
    assertTrue(flCopy.getFilters().get(2) instanceof KeyOnlyFilter);
  }

  /**
   * This test case tests the working of the AssignmentPlan serialization and
   * de-serialization via thrift.
   * @throws Exception
   */
  @Test
  public void testAssignmentPlanSerDe() throws Exception {
    AssignmentPlan originalPlan = new AssignmentPlan();
    List<HServerAddress> lst = new ArrayList<HServerAddress>();
    HTableDescriptor desc = new HTableDescriptor(
        Bytes.toBytes("testAssignmentPlan"),
        new ArrayList<HColumnDescriptor>(),
        new HashMap<byte[], byte[]>());
    HRegionInfo info = new HRegionInfo(desc,
        Bytes.toBytes("aaa"), Bytes.toBytes("bbb"), false, 0);
    lst.add(new HServerAddress(
        java.net.InetAddress.getLocalHost().getHostName(), 0));
    originalPlan.updateAssignmentPlan(info, lst);
    byte[] data = Bytes.writeThriftBytes(originalPlan, AssignmentPlan.class);
    AssignmentPlan planCopy = Bytes.readThriftBytes(data, AssignmentPlan.class);

    assertEquals(originalPlan, planCopy);
  }

  @Test
  public void testHTDSerDe() throws Exception {
    List<HColumnDescriptor> columns = new ArrayList<>(1);
    Map<byte[], byte[]> values = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    Set<HServerAddress> servers = new HashSet<>(1);
    servers.add(new HServerAddress(java.net.InetAddress.getLocalHost().getHostName(),0));

    columns.add(new HColumnDescriptor(Bytes.toBytes("d")));

    HTableDescriptor origionalHTD = new HTableDescriptor(
        Bytes.toBytes("testSerdeName"),
        columns,
        values
    );
    origionalHTD.setServers(servers);

    byte[] data = Bytes.writeThriftBytes(origionalHTD, HTableDescriptor.class);
    HTableDescriptor htdCopy = Bytes.readThriftBytes(data, HTableDescriptor.class);
    assertEquals(origionalHTD, htdCopy);
    assertEquals(servers, htdCopy.getServers());
  }

  @Test
  public void testHFileHistogramSerDe() throws Exception {
    byte [] startRow = Bytes.toBytes("testStartRow");
    byte [] endRow = Bytes.toBytes("testEndRow");
    int cnt = 827946;
    Bucket b = new Bucket.Builder()
      .setStartRow(startRow)
      .setEndRow(endRow)
      .setNumRows(cnt)
      .addHFileStat(HFileStat.KEYVALUECOUNT, 8726.0)
      .create();
    Bucket bCopy = Bytes.readThriftBytes(
        Bytes.writeThriftBytes(b, Bucket.class), Bucket.class);
    assertEquals(b, bCopy);
  }
}
