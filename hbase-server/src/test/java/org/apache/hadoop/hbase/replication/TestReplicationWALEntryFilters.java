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

package org.apache.hadoop.hbase.replication;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@Category({ReplicationTests.class, SmallTests.class})
public class TestReplicationWALEntryFilters {

  static byte[] a = new byte[] {'a'};
  static byte[] b = new byte[] {'b'};
  static byte[] c = new byte[] {'c'};
  static byte[] d = new byte[] {'d'};

  @Test
  public void testSystemTableWALEntryFilter() {
    SystemTableWALEntryFilter filter = new SystemTableWALEntryFilter();

    // meta
    WALKey key1 = new WALKey( HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes(),
      TableName.META_TABLE_NAME, null);
    Entry metaEntry = new Entry(key1, null);

    assertNull(filter.filter(metaEntry));

    // ns table
    WALKey key2 = new WALKey(new byte[] {}, TableName.NAMESPACE_TABLE_NAME, null);
    Entry nsEntry = new Entry(key2, null);
    assertNull(filter.filter(nsEntry));

    // user table

    WALKey key3 = new WALKey(new byte[] {}, TableName.valueOf("foo"), null);
    Entry userEntry = new Entry(key3, null);

    assertEquals(userEntry, filter.filter(userEntry));
  }

  @Test
  public void testScopeWALEntryFilter() {
    WALEntryFilter filter = new ChainWALEntryFilter(new ScopeWALEntryFilter());

    Entry userEntry = createEntry(null, a, b);
    Entry userEntryA = createEntry(null, a);
    Entry userEntryB = createEntry(null, b);
    Entry userEntryEmpty = createEntry(null);

    // no scopes
    assertEquals(null, filter.filter(userEntry));

    // empty scopes
    TreeMap<byte[], Integer> scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    userEntry = createEntry(scopes, a, b);
    assertEquals(null, filter.filter(userEntry));

    // different scope
    scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    scopes.put(c, HConstants.REPLICATION_SCOPE_GLOBAL);
    userEntry = createEntry(scopes, a, b);
    // all kvs should be filtered
    assertEquals(userEntryEmpty, filter.filter(userEntry));

    // local scope
    scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    scopes.put(a, HConstants.REPLICATION_SCOPE_LOCAL);
    userEntry = createEntry(scopes, a, b);
    assertEquals(userEntryEmpty, filter.filter(userEntry));
    scopes.put(b, HConstants.REPLICATION_SCOPE_LOCAL);
    assertEquals(userEntryEmpty, filter.filter(userEntry));

    // only scope a
    scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    scopes.put(a, HConstants.REPLICATION_SCOPE_GLOBAL);
    userEntry = createEntry(scopes, a, b);
    assertEquals(userEntryA, filter.filter(userEntry));
    scopes.put(b, HConstants.REPLICATION_SCOPE_LOCAL);
    assertEquals(userEntryA, filter.filter(userEntry));

    // only scope b
    scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    scopes.put(b, HConstants.REPLICATION_SCOPE_GLOBAL);
    userEntry = createEntry(scopes, a, b);
    assertEquals(userEntryB, filter.filter(userEntry));
    scopes.put(a, HConstants.REPLICATION_SCOPE_LOCAL);
    assertEquals(userEntryB, filter.filter(userEntry));

    // scope a and b
    scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
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

  @Test
  public void testChainWALEntryFilter() {
    Entry userEntry = createEntry(null, a, b, c);

    ChainWALEntryFilter filter = new ChainWALEntryFilter(passFilter);
    assertEquals(createEntry(null, a,b,c), filter.filter(userEntry));

    filter = new ChainWALEntryFilter(passFilter, passFilter);
    assertEquals(createEntry(null, a,b,c), filter.filter(userEntry));

    filter = new ChainWALEntryFilter(passFilter, passFilter, passFilter);
    assertEquals(createEntry(null, a,b,c), filter.filter(userEntry));

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
    filter =
        new ChainWALEntryFilter(
          new ChainWALEntryFilter(passFilter,
            new ChainWALEntryFilter(passFilter, passFilter),
          new ChainWALEntryFilter(passFilter),
          new ChainWALEntryFilter(passFilter)),
          new ChainWALEntryFilter(passFilter));
    assertEquals(createEntry(null, a,b,c), filter.filter(userEntry));


    filter =
        new ChainWALEntryFilter(
          new ChainWALEntryFilter(passFilter,
            new ChainWALEntryFilter(passFilter,
              new ChainWALEntryFilter(nullFilter))),
          new ChainWALEntryFilter(passFilter));
    assertEquals(null, filter.filter(userEntry));
  }

  @Test
  public void testNamespaceTableCfWALEntryFilter() {
    ReplicationPeer peer = mock(ReplicationPeer.class);

    // 1. no namespaces config and table-cfs config in peer
    when(peer.getNamespaces()).thenReturn(null);
    when(peer.getTableCFs()).thenReturn(null);
    Entry userEntry = createEntry(null, a, b, c);
    WALEntryFilter filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, a,b,c), filter.filter(userEntry));

    // 2. Only config table-cfs in peer
    // empty map
    userEntry = createEntry(null, a, b, c);
    Map<TableName, List<String>> tableCfs = new HashMap<TableName, List<String>>();
    when(peer.getTableCFs()).thenReturn(tableCfs);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(null, filter.filter(userEntry));

    // table bar
    userEntry = createEntry(null, a, b, c);
    tableCfs = new HashMap<TableName, List<String>>();
    tableCfs.put(TableName.valueOf("bar"), null);
    when(peer.getTableCFs()).thenReturn(tableCfs);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(null, filter.filter(userEntry));

    // table foo:a
    userEntry = createEntry(null, a, b, c);
    tableCfs = new HashMap<TableName, List<String>>();
    tableCfs.put(TableName.valueOf("foo"), Lists.newArrayList("a"));
    when(peer.getTableCFs()).thenReturn(tableCfs);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, a), filter.filter(userEntry));

    // table foo:a,c
    userEntry = createEntry(null, a, b, c, d);
    tableCfs = new HashMap<TableName, List<String>>();
    tableCfs.put(TableName.valueOf("foo"), Lists.newArrayList("a", "c"));
    when(peer.getTableCFs()).thenReturn(tableCfs);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, a,c), filter.filter(userEntry));

    // 3. Only config namespaces in peer
    when(peer.getTableCFs()).thenReturn(null);
    // empty set
    Set<String> namespaces = new HashSet<String>();
    when(peer.getNamespaces()).thenReturn(namespaces);
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(null, filter.filter(userEntry));

    // namespace default
    namespaces.add("default");
    when(peer.getNamespaces()).thenReturn(namespaces);
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, a,b,c), filter.filter(userEntry));

    // namespace ns1
    namespaces = new HashSet<String>();;
    namespaces.add("ns1");
    when(peer.getNamespaces()).thenReturn(namespaces);
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(null, filter.filter(userEntry));

    // 4. Config namespaces and table-cfs both
    // Namespaces config should not confict with table-cfs config
    namespaces = new HashSet<String>();
    tableCfs = new HashMap<TableName, List<String>>();
    namespaces.add("ns1");
    when(peer.getNamespaces()).thenReturn(namespaces);
    tableCfs.put(TableName.valueOf("foo"), Lists.newArrayList("a", "c"));
    when(peer.getTableCFs()).thenReturn(tableCfs);
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, a, c), filter.filter(userEntry));

    namespaces = new HashSet<String>();;
    tableCfs = new HashMap<TableName, List<String>>();
    namespaces.add("default");
    when(peer.getNamespaces()).thenReturn(namespaces);
    tableCfs.put(TableName.valueOf("ns1:foo"), Lists.newArrayList("a", "c"));
    when(peer.getTableCFs()).thenReturn(tableCfs);
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(createEntry(null, a, b, c), filter.filter(userEntry));

    namespaces = new HashSet<String>();;
    tableCfs = new HashMap<TableName, List<String>>();
    namespaces.add("ns1");
    when(peer.getNamespaces()).thenReturn(namespaces);
    tableCfs.put(TableName.valueOf("bar"), null);
    when(peer.getTableCFs()).thenReturn(tableCfs);
    userEntry = createEntry(null, a, b, c);
    filter = new ChainWALEntryFilter(new NamespaceTableCfWALEntryFilter(peer));
    assertEquals(null, filter.filter(userEntry));
  }

  private Entry createEntry(TreeMap<byte[], Integer> scopes, byte[]... kvs) {
    WALKey key1 = new WALKey(new byte[] {}, TableName.valueOf("foo"), scopes);
    WALEdit edit1 = new WALEdit();

    for (byte[] kv : kvs) {
      edit1.add(new KeyValue(kv, kv, kv));
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
      CellComparator.COMPARATOR.compare(cells1.get(i), cells2.get(i));
    }
  }
}
