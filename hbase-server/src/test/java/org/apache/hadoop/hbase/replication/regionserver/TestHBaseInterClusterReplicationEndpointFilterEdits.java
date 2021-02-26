/**
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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint.Context;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Tests {@link HBaseInterClusterReplicationEndpoint#filterNotExistColumnFamilyEdits(List)} and
 * {@link HBaseInterClusterReplicationEndpoint#filterNotExistTableEdits(List)}
 */
@Category({ ReplicationTests.class, MediumTests.class })
public class TestHBaseInterClusterReplicationEndpointFilterEdits {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseInterClusterReplicationEndpointFilterEdits.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static HBaseInterClusterReplicationEndpoint endpoint;

  private static final TableName TABLE1 = TableName.valueOf("T1");
  private static final TableName TABLE2 = TableName.valueOf("T2");

  private static final byte[] FAMILY = Bytes.toBytes("CF");
  private static final byte[] NON_EXISTING_FAMILY = Bytes.toBytes("NECF");
  private static final byte[] QUALIFIER = Bytes.toBytes("Q");
  private static final byte[] ROW = Bytes.toBytes("r");
  private static final byte[] VALUE = Bytes.toBytes("v");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster();
    ReplicationPeer replicationPeer = mock(ReplicationPeer.class);
    ReplicationPeerConfig rpc = mock(ReplicationPeerConfig.class);
    when(rpc.isSerial()).thenReturn(false);
    when(replicationPeer.getPeerConfig()).thenReturn(rpc);
    Context context = new Context(UTIL.getConfiguration(), UTIL.getConfiguration(), null,
        null, null, replicationPeer, null, null, null);
    endpoint = new HBaseInterClusterReplicationEndpoint();
    endpoint.init(context);

    UTIL.createTable(TABLE1, FAMILY);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testFilterNotExistColumnFamilyEdits() {
    List<List<Entry>> entryList = new ArrayList<>();
    // should be filtered
    Cell c1 = new KeyValue(ROW, NON_EXISTING_FAMILY, QUALIFIER, System.currentTimeMillis(),
        Type.Put, VALUE);
    Entry e1 = new Entry(new WALKeyImpl(new byte[32], TABLE1, System.currentTimeMillis()),
        new WALEdit().add(c1));
    entryList.add(Lists.newArrayList(e1));
    // should be kept
    Cell c2 = new KeyValue(ROW, FAMILY, QUALIFIER, System.currentTimeMillis(), Type.Put, VALUE);
    Entry e2 = new Entry(new WALKeyImpl(new byte[32], TABLE1, System.currentTimeMillis()),
        new WALEdit().add(c2));
    entryList.add(Lists.newArrayList(e2, e1));
    List<List<Entry>> filtered = endpoint.filterNotExistColumnFamilyEdits(entryList);
    assertEquals(1, filtered.size());
    assertEquals(1, filtered.get(0).get(0).getEdit().getCells().size());
    Cell cell = filtered.get(0).get(0).getEdit().getCells().get(0);
    assertTrue(CellUtil.matchingFamily(cell, FAMILY));
  }

  @Test
  public void testFilterNotExistTableEdits() {
    List<List<Entry>> entryList = new ArrayList<>();
    // should be filtered
    Cell c1 = new KeyValue(ROW, FAMILY, QUALIFIER, System.currentTimeMillis(), Type.Put, VALUE);
    Entry e1 = new Entry(new WALKeyImpl(new byte[32], TABLE2, System.currentTimeMillis()),
        new WALEdit().add(c1));
    entryList.add(Lists.newArrayList(e1));
    // should be kept
    Cell c2 = new KeyValue(ROW, FAMILY, QUALIFIER, System.currentTimeMillis(), Type.Put, VALUE);
    Entry e2 = new Entry(new WALKeyImpl(new byte[32], TABLE1, System.currentTimeMillis()),
        new WALEdit().add(c2));
    entryList.add(Lists.newArrayList(e2));
    List<List<Entry>> filtered = endpoint.filterNotExistTableEdits(entryList);
    assertEquals(1, filtered.size());
    Entry entry = filtered.get(0).get(0);
    assertEquals(1, entry.getEdit().getCells().size());
    assertEquals(TABLE1, entry.getKey().getTableName());
  }

}
