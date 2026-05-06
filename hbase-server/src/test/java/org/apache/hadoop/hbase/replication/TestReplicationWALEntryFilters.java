/*
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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.security.access.PermissionStorage;
import org.apache.hadoop.hbase.security.visibility.VisibilityConstants;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALEditInternalHelper;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category({ ReplicationTests.class, SmallTests.class })
public class TestReplicationWALEntryFilters {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationWALEntryFilters.class);

  static byte[] a = new byte[] { 'a' };
  static byte[] b = new byte[] { 'b' };
  static byte[] c = new byte[] { 'c' };
  static byte[] d = new byte[] { 'd' };

  @Test
  public void testSystemTableWALEntryFilter() {
    SystemTableWALEntryFilter filter = new SystemTableWALEntryFilter();

    // meta
    WALKeyImpl key1 =
      new WALKeyImpl(RegionInfoBuilder.FIRST_META_REGIONINFO.getEncodedNameAsBytes(),
        TableName.META_TABLE_NAME, EnvironmentEdgeManager.currentTime());
    Entry metaEntry = new Entry(key1, null);

    assertNull(filter.filter(metaEntry));

    // user table
    WALKeyImpl key3 =
      new WALKeyImpl(new byte[0], TableName.valueOf("foo"), EnvironmentEdgeManager.currentTime());
    Entry userEntry = new Entry(key3, null);

    assertEquals(userEntry, filter.filter(userEntry));

    // hbase:acl should be allowed through the filter
    WALKeyImpl key4 =
      new WALKeyImpl(new byte[0], PermissionStorage.ACL_TABLE_NAME, System.currentTimeMillis());
    Entry aclEntry = new Entry(key4, null);
    assertEquals(aclEntry, filter.filter(aclEntry));

    // hbase:labels should be allowed through the filter
    WALKeyImpl key5 = new WALKeyImpl(new byte[0], VisibilityConstants.LABELS_TABLE_NAME,
      System.currentTimeMillis());
    Entry labelsEntry = new Entry(key5, null);
    assertEquals(labelsEntry, filter.filter(labelsEntry));
  }

  @Test
  public void testScopeWALEntryFilter() {
    WALEntryFilter filter = new ChainWALEntryFilter(new ScopeWALEntryFilter());

    Entry userEntry = createEntry(null, a, b);
    Entry userEntryA = createEntry(null, a);
    Entry userEntryB = createEntry(null, b);
    Entry userEntryEmpty = createEntry(null);

    // no scopes
    assertNull(filter.filter(userEntry));
    // now for serial replication, we will not filter out entries without a replication scope since
    // serial replication still need the sequence id, but the cells will all be filtered out.
    filter.setSerial(true);
    assertTrue(filter.filter(userEntry).getEdit().isEmpty());
    filter.setSerial(false);

    // empty scopes
    TreeMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    userEntry = createEntry(scopes, a, b);
    assertNull(filter.filter(userEntry));
    filter.setSerial(true);
    assertTrue(filter.filter(userEntry).getEdit().isEmpty());
    filter.setSerial(false);

    // different scope
    scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    scopes.put(c, HConstants.REPLICATION_SCOPE_GLOBAL);
    userEntry = createEntry(scopes, a, b);
    // all kvs should be filtered
    assertEquals(userEntryEmpty, filter.filter(userEntry));

    // local scope
    scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    scopes.put(a, HConstants.REPLICATION_SCOPE_LOCAL);
    userEntry = createEntry(scopes, a, b);
    assertEquals(userEntryEmpty, filter.filter(userEntry));
    scopes.put(b, HConstants.REPLICATION_SCOPE_LOCAL);
    assertEquals(userEntryEmpty, filter.filter(userEntry));

    // only scope a
    scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    scopes.put(a, HConstants.REPLICATION_SCOPE_GLOBAL);
    userEntry = createEntry(scopes, a, b);
    assertEquals(userEntryA, filter.filter(userEntry));
    scopes.put(b, HConstants.REPLICATION_SCOPE_LOCAL);
    assertEquals(userEntryA, filter.filter(userEntry));

    // only scope b
    scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    scopes.put(b, HConstants.REPLICATION_SCOPE_GLOBAL);
    userEntry = createEntry(scopes, a, b);
    assertEquals(userEntryB, filter.filter(userEntry));
    scopes.put(a, HConstants.REPLICATION_SCOPE_LOCAL);
    assertEquals(userEntryB, filter.filter(userEntry));

    // scope a and b
    scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    scopes.put(b, HConstants.REPLICATION_SCOPE_GLOBAL);
    userEntry = createEntry(scopes, a, b);
    assertEquals(userEntryB, filter.filter(userEntry));
    scopes.put(a, HConstants.REPLICATION_SCOPE_LOCAL);
    assertEquals(userEntryB, filter.filter(userEntry));
  }

  WALEntryFilter nullFilter = new WALEntryFilter() {
    @Override
    public Entry filter(Entry entry) {
      return null;
    }
  };

  WALEntryFilter passFilter = new WALEntryFilter() {
    @Override
    public Entry filter(Entry entry) {
      return entry;
    }
  };

  public static class FilterSomeCellsWALCellFilter implements WALEntryFilter, WALCellFilter {
    @Override
    public Entry filter(Entry entry) {
      return entry;
    }

    @Override
    public Cell filterCell(Entry entry, Cell cell) {
      if (
        Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()).equals("a")
      ) {
        return null;
      } else {
        return cell;
      }
    }
  }

  public static class FilterAllCellsWALCellFilter implements WALEntryFilter, WALCellFilter {
    @Override
    public Entry filter(Entry entry) {
      return entry;
    }

    @Override
    public Cell filterCell(Entry entry, Cell cell) {
      return null;
    }
  }

  @Test
  public void testChainWALEntryWithCellFilter() {
    Entry userEntry = createEntry(null, a, b, c);
    ChainWALEntryFilter filterSomeCells =
      new ChainWALEntryFilter(new FilterSomeCellsWALCellFilter());
    // since WALCellFilter filter cells with rowkey 'a'
    assertEquals(createEntry(null, b, c), filterSomeCells.filter(userEntry));

    Entry userEntry2 = createEntry(null, b, c, d);
    // since there is no cell to get filtered, nothing should get filtered
    assertEquals(userEntry2, filterSomeCells.filter(userEntry2));

    // since we filter all the cells, we should get empty entry
    ChainWALEntryFilter filterAllCells = new ChainWALEntryFilter(new FilterAllCellsWALCellFilter());
    assertEquals(createEntry(null), filterAllCells.filter(userEntry));
  }

  @Test
  public void testChainWALEmptyEntryWithCellFilter() {
    Entry userEntry = createEntry(null, a, b, c);
    ChainWALEmptyEntryFilter filterSomeCells =
      new ChainWALEmptyEntryFilter(new FilterSomeCellsWALCellFilter());
    // since WALCellFilter filter cells with rowkey 'a'
    assertEquals(createEntry(null, b, c), filterSomeCells.filter(userEntry));

    Entry userEntry2 = createEntry(null, b, c, d);
    // since there is no cell to get filtered, nothing should get filtered
    assertEquals(userEntry2, filterSomeCells.filter(userEntry2));

    ChainWALEmptyEntryFilter filterAllCells =
      new ChainWALEmptyEntryFilter(new FilterAllCellsWALCellFilter());
    assertEquals(createEntry(null), filterAllCells.filter(userEntry));
    // let's set the filter empty entry flag to true now for the above case
    filterAllCells.setFilterEmptyEntry(true);
    // since WALCellFilter filter all cells, whole entry should be filtered
    assertEquals(null, filterAllCells.filter(userEntry));
  }

  @Test
  public void testChainWALEntryFilter() {
    Entry userEntry = createEntry(null, a, b, c);

    ChainWALEntryFilter filter = new ChainWALEntryFilter(passFilter);
    assertEquals(createEntry(null, a, b, c), filter.filter(userEntry));

    filter = new ChainWALEntryFilter(passFilter, passFilter);
    assertEquals(createEntry(null, a, b, c), filter.filter(userEntry));

    filter = new ChainWALEntryFilter(passFilter, passFilter, passFilter);
    assertEquals(createEntry(null, a, b, c), filter.filter(userEntry));

    filter = new ChainWALEntryFilter(nullFilter);
    assertEquals(null, filter.filter(userEntry));

    filter = new ChainWALEntryFilter(nullFilter, passFilter);
    assertEquals(null, filter.filter(userEntry));

    filter = new ChainWALEntryFilter(passFilter, nullFilter);
    assertEquals(null, filter.filter(userEntry));

    filter = new ChainWALEntryFilter(nullFilter, passFilter, nullFilter);
    assertEquals(null, filter.filter(userEntry));

    filter = new ChainWALEntryFilter(nullFilter, nullFilter);
    assertEquals(null, filter.filter(userEntry));

    // flatten
    filter = new ChainWALEntryFilter(
      new ChainWALEntryFilter(passFilter, new ChainWALEntryFilter(passFilter, passFilter),
        new ChainWALEntryFilter(passFilter), new ChainWALEntryFilter(passFilter)),
      new ChainWALEntryFilter(passFilter));
    assertEquals(createEntry(null, a, b, c), filter.filter(userEntry));

    filter = new ChainWALEntryFilter(
      new ChainWALEntryFilter(passFilter,
        new ChainWALEntryFilter(passFilter, new ChainWALEntryFilter(nullFilter))),
      new ChainWALEntryFilter(passFilter));
    assertEquals(null, filter.filter(userEntry));
  }

  @Test
  public void testNamespaceTableCfWALEntryFilter() {
    ReplicationPeer peer = mock(ReplicationPeer.class);
    ReplicationPeerConfigBuilder peerConfigBuilder = ReplicationPeerConfig.newBuilder();

    // 1. replicate_all flag is false, no namespaces and table-cfs config
    peerConfigBuilder.setReplicateAllUserTables(false).setNamespaces(null).setTableCFsMap(null);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    Entry userEntry = createEntry(null, a, b, c);
    ChainWALEntryFilter filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(null, filter.filter(userEntry));

    // 2. replicate_all flag is false, and only config table-cfs in peer
    // empty map
    userEntry = createEntry(null, a, b, c);
    Map<TableName, List<String>> tableCfs = new HashMap<>();
    peerConfigBuilder.setReplicateAllUserTables(false).setTableCFsMap(tableCfs);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(null, filter.filter(userEntry));

    // table bar
    userEntry = createEntry(null, a, b, c);
    tableCfs = new HashMap<>();
    tableCfs.put(TableName.valueOf("bar"), null);
    peerConfigBuilder.setReplicateAllUserTables(false).setTableCFsMap(tableCfs);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(null, filter.filter(userEntry));

    // table foo:a
    userEntry = createEntry(null, a, b, c);
    tableCfs = new HashMap<>();
    tableCfs.put(TableName.valueOf("foo"), Lists.newArrayList("a"));
    peerConfigBuilder.setReplicateAllUserTables(false).setTableCFsMap(tableCfs);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, a), filter.filter(userEntry));

    // table foo:a,c
    userEntry = createEntry(null, a, b, c, d);
    tableCfs = new HashMap<>();
    tableCfs.put(TableName.valueOf("foo"), Lists.newArrayList("a", "c"));
    peerConfigBuilder.setReplicateAllUserTables(false).setTableCFsMap(tableCfs);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, a, c), filter.filter(userEntry));

    // 3. replicate_all flag is false, and only config namespaces in peer
    when(peer.getTableCFs()).thenReturn(null);
    // empty set
    Set<String> namespaces = new HashSet<>();
    peerConfigBuilder.setReplicateAllUserTables(false).setNamespaces(namespaces)
      .setTableCFsMap(null);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(null, filter.filter(userEntry));

    // namespace default
    namespaces.add("default");
    peerConfigBuilder.setReplicateAllUserTables(false).setNamespaces(namespaces);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, a, b, c), filter.filter(userEntry));

    // namespace ns1
    namespaces = new HashSet<>();
    namespaces.add("ns1");
    peerConfigBuilder.setReplicateAllUserTables(false).setNamespaces(namespaces);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(null, filter.filter(userEntry));

    // 4. replicate_all flag is false, and config namespaces and table-cfs both
    // Namespaces config should not confict with table-cfs config
    namespaces = new HashSet<>();
    tableCfs = new HashMap<>();
    namespaces.add("ns1");
    tableCfs.put(TableName.valueOf("foo"), Lists.newArrayList("a", "c"));
    peerConfigBuilder.setReplicateAllUserTables(false).setNamespaces(namespaces)
      .setTableCFsMap(tableCfs);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, a, c), filter.filter(userEntry));

    namespaces = new HashSet<>();
    tableCfs = new HashMap<>();
    namespaces.add("default");
    tableCfs.put(TableName.valueOf("ns1:foo"), Lists.newArrayList("a", "c"));
    peerConfigBuilder.setReplicateAllUserTables(false).setNamespaces(namespaces)
      .setTableCFsMap(tableCfs);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, a, b, c), filter.filter(userEntry));

    namespaces = new HashSet<>();
    tableCfs = new HashMap<>();
    namespaces.add("ns1");
    tableCfs.put(TableName.valueOf("bar"), null);
    peerConfigBuilder.setReplicateAllUserTables(false).setNamespaces(namespaces)
      .setTableCFsMap(tableCfs);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(null, filter.filter(userEntry));
  }

  @Test
  public void testNamespaceTableCfWALEntryFilter2() {
    ReplicationPeer peer = mock(ReplicationPeer.class);
    ReplicationPeerConfigBuilder peerConfigBuilder = ReplicationPeerConfig.newBuilder();

    // 1. replicate_all flag is true
    // and no exclude namespaces and no exclude table-cfs config
    peerConfigBuilder.setReplicateAllUserTables(true).setExcludeNamespaces(null)
      .setExcludeTableCFsMap(null);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    Entry userEntry = createEntry(null, a, b, c);
    ChainWALEntryFilter filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, a, b, c), filter.filter(userEntry));

    // 2. replicate_all flag is true, and only config exclude namespaces
    // empty set
    Set<String> namespaces = new HashSet<String>();
    peerConfigBuilder.setExcludeNamespaces(namespaces).setExcludeTableCFsMap(null);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, a, b, c), filter.filter(userEntry));

    // exclude namespace default
    namespaces.add("default");
    peerConfigBuilder.setExcludeNamespaces(namespaces).setExcludeTableCFsMap(null);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(null, filter.filter(userEntry));

    // exclude namespace ns1
    namespaces = new HashSet<String>();
    namespaces.add("ns1");
    peerConfigBuilder.setExcludeNamespaces(namespaces).setExcludeTableCFsMap(null);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, a, b, c), filter.filter(userEntry));

    // 3. replicate_all flag is true, and only config exclude table-cfs
    // empty table-cfs map
    Map<TableName, List<String>> tableCfs = new HashMap<TableName, List<String>>();
    peerConfigBuilder.setExcludeNamespaces(null).setExcludeTableCFsMap(tableCfs);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, a, b, c), filter.filter(userEntry));

    // exclude table bar
    tableCfs = new HashMap<TableName, List<String>>();
    tableCfs.put(TableName.valueOf("bar"), null);
    peerConfigBuilder.setExcludeNamespaces(null).setExcludeTableCFsMap(tableCfs);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, a, b, c), filter.filter(userEntry));

    // exclude table foo:a
    tableCfs = new HashMap<TableName, List<String>>();
    tableCfs.put(TableName.valueOf("foo"), Lists.newArrayList("a"));
    peerConfigBuilder.setExcludeNamespaces(null).setExcludeTableCFsMap(tableCfs);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, b, c), filter.filter(userEntry));

    // 4. replicate_all flag is true, and config exclude namespaces and table-cfs both
    // exclude ns1 and table foo:a,c
    namespaces = new HashSet<String>();
    tableCfs = new HashMap<TableName, List<String>>();
    namespaces.add("ns1");
    tableCfs.put(TableName.valueOf("foo"), Lists.newArrayList("a", "c"));
    peerConfigBuilder.setExcludeNamespaces(namespaces).setExcludeTableCFsMap(tableCfs);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, b), filter.filter(userEntry));

    // exclude namespace default and table ns1:bar
    namespaces = new HashSet<String>();
    tableCfs = new HashMap<TableName, List<String>>();
    namespaces.add("default");
    tableCfs.put(TableName.valueOf("ns1:bar"), new ArrayList<String>());
    peerConfigBuilder.setExcludeNamespaces(namespaces).setExcludeTableCFsMap(tableCfs);
    when(peer.getPeerConfig()).thenReturn(peerConfigBuilder.build());
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(null, filter.filter(userEntry));
  }

  @Test
  public void testClusterMarkingEntryFilter() {
    // Setup cluster IDs
    UUID clusterId = UUID.randomUUID();
    UUID peerClusterId = UUID.randomUUID();

    // Mock ReplicationEndpoint
    ReplicationEndpoint endpoint = mock(ReplicationEndpoint.class);
    when(endpoint.canReplicateToSameCluster()).thenReturn(false);

    ClusterMarkingEntryFilter filter =
      new ClusterMarkingEntryFilter(clusterId, peerClusterId, endpoint);

    // 1. Entry without any cluster IDs - should pass and be marked with clusterId
    List<UUID> emptyIds = new ArrayList<>();
    WALKeyImpl key1 = new WALKeyImpl(new byte[0], TableName.valueOf("foo"),
      EnvironmentEdgeManager.currentTime(), emptyIds, null, null, null);
    WALEdit edit1 = new WALEdit();
    edit1.add(new KeyValue(a, a, a));
    Entry entry1 = new Entry(key1, edit1);

    Entry filtered1 = filter.filter(entry1);
    Assert.assertNotNull(filtered1);
    Assert.assertTrue(filtered1.getKey().getClusterIds().contains(clusterId));

    // 2. Entry with peerClusterId - should be filtered out (prevent circular replication)
    List<UUID> peerIds = new ArrayList<>();
    peerIds.add(peerClusterId);
    WALKeyImpl key2 = new WALKeyImpl(new byte[0], TableName.valueOf("foo"),
      EnvironmentEdgeManager.currentTime(), peerIds, null, null, null);
    WALEdit edit2 = new WALEdit();
    edit2.add(new KeyValue(a, a, a));
    Entry entry2 = new Entry(key2, edit2);

    Assert.assertNull(filter.filter(entry2));

    // 3. Entry with empty WALEdit - should be filtered out
    WALKeyImpl key3 = new WALKeyImpl(new byte[0], TableName.valueOf("foo"),
      EnvironmentEdgeManager.currentTime(), new ArrayList<>(), null, null, null);
    WALEdit edit3 = new WALEdit();
    Entry entry3 = new Entry(key3, edit3);

    Assert.assertNull(filter.filter(entry3));

    // 4. Entry with null WALEdit - should be filtered out
    WALKeyImpl key4 = new WALKeyImpl(new byte[0], TableName.valueOf("foo"),
      EnvironmentEdgeManager.currentTime(), new ArrayList<>(), null, null, null);
    Entry entry4 = new Entry(key4, null);

    Assert.assertNull(filter.filter(entry4));
  }

  private Entry createEntry(TreeMap<byte[], Integer> scopes, byte[]... kvs) {
    WALKeyImpl key1 = new WALKeyImpl(new byte[0], TableName.valueOf("foo"),
      EnvironmentEdgeManager.currentTime(), scopes);
    WALEdit edit1 = new WALEdit();

    for (byte[] kv : kvs) {
      WALEditInternalHelper.addExtendedCell(edit1, new KeyValue(kv, kv, kv));
    }
    return new Entry(key1, edit1);
  }

  private void assertEquals(Entry e1, Entry e2) {
    Assert.assertEquals(e1 == null, e2 == null);
    if (e1 == null) {
      return;
    }

    // do not compare WALKeys

    // compare kvs
    Assert.assertEquals(e1.getEdit() == null, e2.getEdit() == null);
    if (e1.getEdit() == null) {
      return;
    }
    List<Cell> cells1 = e1.getEdit().getCells();
    List<Cell> cells2 = e2.getEdit().getCells();
    Assert.assertEquals(cells1.size(), cells2.size());
    for (int i = 0; i < cells1.size(); i++) {
      CellComparatorImpl.COMPARATOR.compare(cells1.get(i), cells2.get(i));
    }
  }
}
