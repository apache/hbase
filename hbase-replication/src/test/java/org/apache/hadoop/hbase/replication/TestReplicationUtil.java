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
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, SmallTests.class })
public class TestReplicationUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationUtil.class);

  private static TableName TABLE_A = TableName.valueOf("replication", "testA");
  private static TableName TABLE_B = TableName.valueOf("replication", "testB");

  @Test
  public void testContainsWithReplicatingAll() {
    ReplicationPeerConfig peerConfig;
    ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl builder =
      new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl();
    Map<TableName, List<String>> tableCfs = new HashMap<>();
    Set<String> namespaces = new HashSet<>();

    // 1. replication_all flag is true, no namespaces and table-cfs config
    builder.setReplicateAllUserTables(true);
    peerConfig = builder.build();
    Assert.assertTrue(ReplicationUtils.contains(peerConfig, TABLE_A));

    // 2. replicate_all flag is true, and config in excludedTableCfs
    builder.setExcludeNamespaces(null);
    // empty map
    tableCfs = new HashMap<>();
    builder.setReplicateAllUserTables(true);
    builder.setExcludeTableCFsMap(tableCfs);
    peerConfig = builder.build();
    Assert.assertTrue(ReplicationUtils.contains(peerConfig, TABLE_A));

    // table testB
    tableCfs = new HashMap<>();
    tableCfs.put(TABLE_B, null);
    builder.setReplicateAllUserTables(true);
    builder.setExcludeTableCFsMap(tableCfs);
    peerConfig = builder.build();
    Assert.assertTrue(ReplicationUtils.contains(peerConfig, TABLE_A));

    // table testA
    tableCfs = new HashMap<>();
    tableCfs.put(TABLE_A, null);
    builder.setReplicateAllUserTables(true);
    builder.setExcludeTableCFsMap(tableCfs);
    peerConfig = builder.build();
    Assert.assertFalse(ReplicationUtils.contains(peerConfig, TABLE_A));

    // 3. replicate_all flag is true, and config in excludeNamespaces
    builder.setExcludeTableCFsMap(null);
    // empty set
    namespaces = new HashSet<>();
    builder.setReplicateAllUserTables(true);
    builder.setExcludeNamespaces(namespaces);
    peerConfig = builder.build();
    Assert.assertTrue(ReplicationUtils.contains(peerConfig, TABLE_A));

    // namespace default
    namespaces = new HashSet<>();
    namespaces.add("default");
    builder.setReplicateAllUserTables(true);
    builder.setExcludeNamespaces(namespaces);
    peerConfig = builder.build();
    Assert.assertTrue(ReplicationUtils.contains(peerConfig, TABLE_A));

    // namespace replication
    namespaces = new HashSet<>();
    namespaces.add("replication");
    builder.setReplicateAllUserTables(true);
    builder.setExcludeNamespaces(namespaces);
    peerConfig = builder.build();
    Assert.assertFalse(ReplicationUtils.contains(peerConfig, TABLE_A));

    // 4. replicate_all flag is true, and config excludeNamespaces and excludedTableCfs both
    // Namespaces config doesn't conflict with table-cfs config
    namespaces = new HashSet<>();
    tableCfs = new HashMap<>();
    namespaces.add("replication");
    tableCfs.put(TABLE_A, null);
    builder.setReplicateAllUserTables(true);
    builder.setExcludeTableCFsMap(tableCfs);
    builder.setExcludeNamespaces(namespaces);
    peerConfig = builder.build();
    Assert.assertFalse(ReplicationUtils.contains(peerConfig, TABLE_A));

    // Namespaces config conflicts with table-cfs config
    namespaces = new HashSet<>();
    tableCfs = new HashMap<>();
    namespaces.add("default");
    tableCfs.put(TABLE_A, null);
    builder.setReplicateAllUserTables(true);
    builder.setExcludeTableCFsMap(tableCfs);
    builder.setExcludeNamespaces(namespaces);
    peerConfig = builder.build();
    Assert.assertFalse(ReplicationUtils.contains(peerConfig, TABLE_A));

    namespaces = new HashSet<>();
    tableCfs = new HashMap<>();
    namespaces.add("replication");
    tableCfs.put(TABLE_B, null);
    builder.setReplicateAllUserTables(true);
    builder.setExcludeTableCFsMap(tableCfs);
    builder.setExcludeNamespaces(namespaces);
    peerConfig = builder.build();
    Assert.assertFalse(ReplicationUtils.contains(peerConfig, TABLE_A));

  }

  @Test
  public void testContainsWithoutReplicatingAll() {
    ReplicationPeerConfig peerConfig;
    ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl builder =
      new ReplicationPeerConfig.ReplicationPeerConfigBuilderImpl();
    Map<TableName, List<String>> tableCfs = new HashMap<>();
    Set<String> namespaces = new HashSet<>();

    // 1. replication_all flag is false, no namespaces and table-cfs config
    builder.setReplicateAllUserTables(false);
    peerConfig = builder.build();
    Assert.assertFalse(ReplicationUtils.contains(peerConfig, TABLE_A));

    // 2. replicate_all flag is false, and only config table-cfs in peer
    // empty map
    builder.setReplicateAllUserTables(false);
    builder.setTableCFsMap(tableCfs);
    peerConfig = builder.build();
    Assert.assertFalse(ReplicationUtils.contains(peerConfig, TABLE_A));

    // table testB
    tableCfs = new HashMap<>();
    tableCfs.put(TABLE_B, null);
    builder.setReplicateAllUserTables(false);
    builder.setTableCFsMap(tableCfs);
    peerConfig = builder.build();
    Assert.assertFalse(ReplicationUtils.contains(peerConfig, TABLE_A));

    // table testA
    tableCfs = new HashMap<>();
    tableCfs.put(TABLE_A, null);
    builder.setReplicateAllUserTables(false);
    builder.setTableCFsMap(tableCfs);
    peerConfig = builder.build();
    Assert.assertTrue(ReplicationUtils.contains(peerConfig, TABLE_A));

    // 3. replication_all flag is false, and only config namespace in peer
    builder.setTableCFsMap(null);
    // empty set
    builder.setReplicateAllUserTables(false);
    builder.setNamespaces(namespaces);
    peerConfig = builder.build();
    Assert.assertFalse(ReplicationUtils.contains(peerConfig, TABLE_A));

    // namespace default
    namespaces = new HashSet<>();
    namespaces.add("default");
    builder.setReplicateAllUserTables(false);
    builder.setNamespaces(namespaces);
    peerConfig = builder.build();
    Assert.assertFalse(ReplicationUtils.contains(peerConfig, TABLE_A));

    // namespace replication
    namespaces = new HashSet<>();
    namespaces.add("replication");
    builder.setReplicateAllUserTables(false);
    builder.setNamespaces(namespaces);
    peerConfig = builder.build();
    Assert.assertTrue(ReplicationUtils.contains(peerConfig, TABLE_A));

    // 4. replicate_all flag is false, and config namespaces and table-cfs both
    // Namespaces config doesn't conflict with table-cfs config
    namespaces = new HashSet<>();
    tableCfs = new HashMap<>();
    namespaces.add("replication");
    tableCfs.put(TABLE_A, null);
    builder.setReplicateAllUserTables(false);
    builder.setTableCFsMap(tableCfs);
    builder.setNamespaces(namespaces);
    peerConfig = builder.build();
    Assert.assertTrue(ReplicationUtils.contains(peerConfig, TABLE_A));

    // Namespaces config conflicts with table-cfs config
    namespaces = new HashSet<>();
    tableCfs = new HashMap<>();
    namespaces.add("default");
    tableCfs.put(TABLE_A, null);
    builder.setReplicateAllUserTables(false);
    builder.setTableCFsMap(tableCfs);
    builder.setNamespaces(namespaces);
    peerConfig = builder.build();
    Assert.assertTrue(ReplicationUtils.contains(peerConfig, TABLE_A));

    namespaces = new HashSet<>();
    tableCfs = new HashMap<>();
    namespaces.add("replication");
    tableCfs.put(TABLE_B, null);
    builder.setReplicateAllUserTables(false);
    builder.setTableCFsMap(tableCfs);
    builder.setNamespaces(namespaces);
    peerConfig = builder.build();
    Assert.assertTrue(ReplicationUtils.contains(peerConfig, TABLE_A));
  }
}
