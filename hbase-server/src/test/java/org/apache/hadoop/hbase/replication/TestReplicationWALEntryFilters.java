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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@Category(SmallTests.class)
public class TestReplicationWALEntryFilters {

  static byte[] a = new byte[] {'a'};
  static byte[] b = new byte[] {'b'};
  static byte[] c = new byte[] {'c'};
  static byte[] d = new byte[] {'d'};

  @Test
  public void testSystemTableWALEntryFilter() {
    SystemTableWALEntryFilter filter = new SystemTableWALEntryFilter();

    // meta
    HLogKey key1 = new HLogKey( HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes(),
      HTableDescriptor.META_TABLEDESC.getTableName(), 0, 0, null);
    HLog.Entry metaEntry = new Entry(key1, null);

    assertNull(filter.filter(metaEntry));

    // ns table
    HLogKey key2 = new HLogKey(new byte[] {}, HTableDescriptor.NAMESPACE_TABLEDESC.getTableName()
      , 0, 0, null);
    HLog.Entry nsEntry = new Entry(key2, null);
    assertNull(filter.filter(nsEntry));

    // user table

    HLogKey key3 = new HLogKey(new byte[] {}, TableName.valueOf("foo"), 0, 0, null);
    HLog.Entry userEntry = new Entry(key3, null);

    assertEquals(userEntry, filter.filter(userEntry));
  }

  @Test
  public void testScopeWALEntryFilter() {
    ScopeWALEntryFilter filter = new ScopeWALEntryFilter();

    HLog.Entry userEntry = createEntry(a, b);
    HLog.Entry userEntryA = createEntry(a);
    HLog.Entry userEntryB = createEntry(b);
    HLog.Entry userEntryEmpty = createEntry();

    // no scopes
    assertEquals(null, filter.filter(userEntry));

    // empty scopes
    TreeMap<byte[], Integer> scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    userEntry = createEntry(a, b);
    userEntry.getKey().setScopes(scopes);
    assertEquals(null, filter.filter(userEntry));

    // different scope
    scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    scopes.put(c, HConstants.REPLICATION_SCOPE_GLOBAL);
    userEntry = createEntry(a, b);
    userEntry.getKey().setScopes(scopes);
    // all kvs should be filtered
    assertEquals(userEntryEmpty, filter.filter(userEntry));

    // local scope
    scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    scopes.put(a, HConstants.REPLICATION_SCOPE_LOCAL);
    userEntry = createEntry(a, b);
    userEntry.getKey().setScopes(scopes);
    assertEquals(userEntryEmpty, filter.filter(userEntry));
    scopes.put(b, HConstants.REPLICATION_SCOPE_LOCAL);
    assertEquals(userEntryEmpty, filter.filter(userEntry));

    // only scope a
    scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    scopes.put(a, HConstants.REPLICATION_SCOPE_GLOBAL);
    userEntry = createEntry(a, b);
    userEntry.getKey().setScopes(scopes);
    assertEquals(userEntryA, filter.filter(userEntry));
    scopes.put(b, HConstants.REPLICATION_SCOPE_LOCAL);
    assertEquals(userEntryA, filter.filter(userEntry));

    // only scope b
    scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    scopes.put(b, HConstants.REPLICATION_SCOPE_GLOBAL);
    userEntry = createEntry(a, b);
    userEntry.getKey().setScopes(scopes);
    assertEquals(userEntryB, filter.filter(userEntry));
    scopes.put(a, HConstants.REPLICATION_SCOPE_LOCAL);
    assertEquals(userEntryB, filter.filter(userEntry));

    // scope a and b
    scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    scopes.put(b, HConstants.REPLICATION_SCOPE_GLOBAL);
    userEntry = createEntry(a, b);
    userEntry.getKey().setScopes(scopes);
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
    HLog.Entry userEntry = createEntry(a, b, c);

    ChainWALEntryFilter filter = new ChainWALEntryFilter(passFilter);
    assertEquals(createEntry(a,b,c), filter.filter(userEntry));

    filter = new ChainWALEntryFilter(passFilter, passFilter);
    assertEquals(createEntry(a,b,c), filter.filter(userEntry));

    filter = new ChainWALEntryFilter(passFilter, passFilter, passFilter);
    assertEquals(createEntry(a,b,c), filter.filter(userEntry));

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
    assertEquals(createEntry(a,b,c), filter.filter(userEntry));


    filter =
        new ChainWALEntryFilter(
          new ChainWALEntryFilter(passFilter,
            new ChainWALEntryFilter(passFilter,
              new ChainWALEntryFilter(nullFilter))),
          new ChainWALEntryFilter(passFilter));
    assertEquals(null, filter.filter(userEntry));
  }

  @Test
  public void testTableCfWALEntryFilter() {
    ReplicationPeer peer = mock(ReplicationPeer.class);

    when(peer.getTableCFs()).thenReturn(null);
    HLog.Entry userEntry = createEntry(a, b, c);
    TableCfWALEntryFilter filter = new TableCfWALEntryFilter(peer);
    assertEquals(createEntry(a,b,c), filter.filter(userEntry));

    // empty map
    userEntry = createEntry(a, b, c);
    Map<String, List<String>> tableCfs = new HashMap<String, List<String>>();
    when(peer.getTableCFs()).thenReturn(tableCfs);
    filter = new TableCfWALEntryFilter(peer);
    assertEquals(null, filter.filter(userEntry));

    // table bar
    userEntry = createEntry(a, b, c);
    tableCfs = new HashMap<String, List<String>>();
    tableCfs.put("bar", null);
    when(peer.getTableCFs()).thenReturn(tableCfs);
    filter = new TableCfWALEntryFilter(peer);
    assertEquals(null, filter.filter(userEntry));

    // table foo:a
    userEntry = createEntry(a, b, c);
    tableCfs = new HashMap<String, List<String>>();
    tableCfs.put("foo", Lists.newArrayList("a"));
    when(peer.getTableCFs()).thenReturn(tableCfs);
    filter = new TableCfWALEntryFilter(peer);
    assertEquals(createEntry(a), filter.filter(userEntry));

    // table foo:a,c
    userEntry = createEntry(a, b, c, d);
    tableCfs = new HashMap<String, List<String>>();
    tableCfs.put("foo", Lists.newArrayList("a", "c"));
    when(peer.getTableCFs()).thenReturn(tableCfs);
    filter = new TableCfWALEntryFilter(peer);
    assertEquals(createEntry(a,c), filter.filter(userEntry));
  }

  private HLog.Entry createEntry(byte[]... kvs) {
    HLogKey key1 = new HLogKey(new byte[] {}, TableName.valueOf("foo"), 0, 0, null);
    WALEdit edit1 = new WALEdit();

    for (byte[] kv : kvs) {
      edit1.add(new KeyValue(kv, kv, kv));
    }
    return new HLog.Entry(key1, edit1);
  }


  private void assertEquals(HLog.Entry e1, HLog.Entry e2) {
    Assert.assertEquals(e1 == null, e2 == null);
    if (e1 == null) {
      return;
    }

    // do not compare HLogKeys

    // compare kvs
    Assert.assertEquals(e1.getEdit() == null, e2.getEdit() == null);
    if (e1.getEdit() == null) {
      return;
    }
    List<KeyValue> kvs1 = e1.getEdit().getKeyValues();
    List<KeyValue> kvs2 = e2.getEdit().getKeyValues();
    Assert.assertEquals(kvs1.size(), kvs2.size());
    for (int i = 0; i < kvs1.size(); i++) {
      KeyValue.COMPARATOR.compare(kvs1.get(i), kvs2.get(i));
    }
  }


}
