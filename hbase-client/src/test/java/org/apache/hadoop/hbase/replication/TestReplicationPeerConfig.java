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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.BuilderStyleTest;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category({ClientTests.class, SmallTests.class})
public class TestReplicationPeerConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationPeerConfig.class);

  private static final String NAMESPACE_REPLICATE = "replicate";
  private static final String NAMESPACE_OTHER = "other";
  private static final TableName TABLE_A = TableName.valueOf(NAMESPACE_REPLICATE, "testA");
  private static final TableName TABLE_B = TableName.valueOf(NAMESPACE_REPLICATE, "testB");
  private static final byte[] FAMILY1 = Bytes.toBytes("cf1");
  private static final byte[] FAMILY2 = Bytes.toBytes("cf2");

  @Test
  public void testClassMethodsAreBuilderStyle() {
    /* ReplicationPeerConfig should have a builder style setup where setXXX/addXXX methods
     * can be chainable together:
     * . For example:
     * ReplicationPeerConfig htd
     *   = new ReplicationPeerConfig()
     *     .setFoo(foo)
     *     .setBar(bar)
     *     .setBuz(buz)
     *
     * This test ensures that all methods starting with "set" returns the declaring object
     */

    BuilderStyleTest.assertClassesAreBuilderStyle(ReplicationPeerConfig.class);
  }

  @Test
  public void testNeedToReplicateWithReplicatingAll() {
    // 1. replication_all flag is true, no namespaces and table-cfs config
    ReplicationPeerConfig peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(true)
      .build();
    assertTrue(peerConfig.needToReplicate(TABLE_A));

    // 2. replicate_all flag is true, and config in excludedTableCfs
    // Exclude empty table-cfs map
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(true)
      .setExcludeTableCFsMap(Maps.newHashMap())
      .build();
    assertTrue(peerConfig.needToReplicate(TABLE_A));

    // Exclude table B
    Map<TableName, List<String>> tableCfs = Maps.newHashMap();
    tableCfs.put(TABLE_B, null);
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(true)
      .setExcludeTableCFsMap(tableCfs)
      .build();
    assertTrue(peerConfig.needToReplicate(TABLE_A));
    assertFalse(peerConfig.needToReplicate(TABLE_B));

    // 3. replicate_all flag is true, and config in excludeNamespaces
    // Exclude empty namespace set
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(true)
      .setExcludeNamespaces(Sets.newHashSet())
      .build();
    assertTrue(peerConfig.needToReplicate(TABLE_A));

    // Exclude namespace other
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(true)
      .setExcludeNamespaces(Sets.newHashSet(NAMESPACE_OTHER))
      .build();
    assertTrue(peerConfig.needToReplicate(TABLE_A));

    // Exclude namespace replication
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(true)
      .setExcludeNamespaces(Sets.newHashSet(NAMESPACE_REPLICATE))
      .build();
    assertFalse(peerConfig.needToReplicate(TABLE_A));

    // 4. replicate_all flag is true, and config excludeNamespaces and excludedTableCfs both
    // Namespaces config doesn't conflict with table-cfs config
    tableCfs = Maps.newHashMap();
    tableCfs.put(TABLE_A, null);
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(true)
      .setExcludeNamespaces(Sets.newHashSet(NAMESPACE_REPLICATE))
      .setExcludeTableCFsMap(tableCfs)
      .build();
    assertFalse(peerConfig.needToReplicate(TABLE_A));

    // Namespaces config conflicts with table-cfs config
    tableCfs = Maps.newHashMap();
    tableCfs.put(TABLE_A, null);
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(true)
      .setExcludeTableCFsMap(tableCfs)
      .setExcludeNamespaces(Sets.newHashSet(NAMESPACE_OTHER))
      .build();
    assertFalse(peerConfig.needToReplicate(TABLE_A));
    assertTrue(peerConfig.needToReplicate(TABLE_B));

    tableCfs = Maps.newHashMap();
    tableCfs.put(TABLE_B, null);
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(true)
      .setExcludeTableCFsMap(tableCfs)
      .setExcludeNamespaces(Sets.newHashSet(NAMESPACE_REPLICATE))
      .build();
    assertFalse(peerConfig.needToReplicate(TABLE_A));
    assertFalse(peerConfig.needToReplicate(TABLE_B));
  }

  @Test
  public void testNeedToReplicateWithoutReplicatingAll() {
    ReplicationPeerConfig peerConfig;
    Map<TableName, List<String>> tableCfs;

    // 1. replication_all flag is false, no namespaces and table-cfs config
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(false)
      .build();
    assertFalse(peerConfig.needToReplicate(TABLE_A));

    // 2. replicate_all flag is false, and only config table-cfs in peer
    // Set empty table-cfs map
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(false)
      .setTableCFsMap(Maps.newHashMap())
      .build();
    assertFalse(peerConfig.needToReplicate(TABLE_A));

    // Set table B
    tableCfs = Maps.newHashMap();
    tableCfs.put(TABLE_B, null);
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(false)
      .setTableCFsMap(tableCfs)
      .build();
    assertFalse(peerConfig.needToReplicate(TABLE_A));
    assertTrue(peerConfig.needToReplicate(TABLE_B));

    // 3. replication_all flag is false, and only config namespace in peer
    // Set empty namespace set
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(false)
      .setNamespaces(Sets.newHashSet())
      .build();
    assertFalse(peerConfig.needToReplicate(TABLE_A));

    // Set namespace other
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(false)
      .setNamespaces(Sets.newHashSet(NAMESPACE_OTHER))
      .build();
    assertFalse(peerConfig.needToReplicate(TABLE_A));

    // Set namespace replication
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(false)
      .setNamespaces(Sets.newHashSet(NAMESPACE_REPLICATE))
      .build();
    assertTrue(peerConfig.needToReplicate(TABLE_A));

    // 4. replicate_all flag is false, and config namespaces and table-cfs both
    // Namespaces config doesn't conflict with table-cfs config
    tableCfs = Maps.newHashMap();
    tableCfs.put(TABLE_A, null);
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(false)
      .setTableCFsMap(tableCfs)
      .setNamespaces(Sets.newHashSet(NAMESPACE_REPLICATE))
      .build();
    assertTrue(peerConfig.needToReplicate(TABLE_A));

    // Namespaces config conflicts with table-cfs config
    tableCfs = Maps.newHashMap();
    tableCfs.put(TABLE_A, null);
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(false)
      .setTableCFsMap(tableCfs)
      .setNamespaces(Sets.newHashSet(NAMESPACE_OTHER))
      .build();
    assertTrue(peerConfig.needToReplicate(TABLE_A));

    tableCfs = Maps.newHashMap();
    tableCfs.put(TABLE_B, null);
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(false)
      .setNamespaces(Sets.newHashSet(NAMESPACE_REPLICATE))
      .setTableCFsMap(tableCfs)
      .build();
    assertTrue(peerConfig.needToReplicate(TABLE_A));
  }

  @Test
  public void testNeedToReplicateCFWithReplicatingAll() {
    Map<TableName, List<String>> excludeTableCfs = Maps.newHashMap();
    excludeTableCfs.put(TABLE_A, null);
    ReplicationPeerConfig peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(true)
      .setExcludeTableCFsMap(excludeTableCfs)
      .build();
    assertFalse(peerConfig.needToReplicate(TABLE_A));
    assertFalse(peerConfig.needToReplicate(TABLE_A, FAMILY1));
    assertFalse(peerConfig.needToReplicate(TABLE_A, FAMILY2));

    excludeTableCfs = Maps.newHashMap();
    excludeTableCfs.put(TABLE_A, Lists.newArrayList());
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(true)
      .setExcludeTableCFsMap(excludeTableCfs)
      .build();
    assertFalse(peerConfig.needToReplicate(TABLE_A));
    assertFalse(peerConfig.needToReplicate(TABLE_A, FAMILY1));
    assertFalse(peerConfig.needToReplicate(TABLE_A, FAMILY2));

    excludeTableCfs = Maps.newHashMap();
    excludeTableCfs.put(TABLE_A, Lists.newArrayList(Bytes.toString(FAMILY1)));
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(true)
      .setExcludeTableCFsMap(excludeTableCfs)
      .build();
    assertTrue(peerConfig.needToReplicate(TABLE_A));
    assertFalse(peerConfig.needToReplicate(TABLE_A, FAMILY1));
    assertTrue(peerConfig.needToReplicate(TABLE_A, FAMILY2));
  }

  @Test
  public void testNeedToReplicateCFWithoutReplicatingAll() {
    Map<TableName, List<String>> tableCfs = Maps.newHashMap();
    tableCfs.put(TABLE_A, null);
    ReplicationPeerConfig peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(false)
      .setTableCFsMap(tableCfs)
      .build();
    assertTrue(peerConfig.needToReplicate(TABLE_A));
    assertTrue(peerConfig.needToReplicate(TABLE_A, FAMILY1));
    assertTrue(peerConfig.needToReplicate(TABLE_A, FAMILY2));

    tableCfs = Maps.newHashMap();
    tableCfs.put(TABLE_A, Lists.newArrayList());
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(false)
      .setTableCFsMap(tableCfs)
      .build();
    assertTrue(peerConfig.needToReplicate(TABLE_A));
    assertTrue(peerConfig.needToReplicate(TABLE_A, FAMILY1));
    assertTrue(peerConfig.needToReplicate(TABLE_A, FAMILY2));

    tableCfs = Maps.newHashMap();
    tableCfs.put(TABLE_A, Lists.newArrayList(Bytes.toString(FAMILY1)));
    peerConfig = new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl()
      .setReplicateAllUserTables(false)
      .setTableCFsMap(tableCfs)
      .build();
    assertTrue(peerConfig.needToReplicate(TABLE_A));
    assertTrue(peerConfig.needToReplicate(TABLE_A, FAMILY1));
    assertFalse(peerConfig.needToReplicate(TABLE_A, FAMILY2));
  }
}
