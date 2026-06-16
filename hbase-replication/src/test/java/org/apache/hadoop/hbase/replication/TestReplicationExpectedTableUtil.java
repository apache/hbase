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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(ReplicationTests.TAG)
@Tag(SmallTests.TAG)
public class TestReplicationExpectedTableUtil {

  private static final TableName USER_TABLE = TableName.valueOf("ns", "data");
  private static final byte[] CF_GLOBAL = new byte[] { 'g' };
  private static final byte[] CF_LOCAL = new byte[] { 'l' };

  @Test
  public void testExpectedTableRequiresGlobalScopeAndPeerConfig() {
    TableDescriptor globalCf = table(USER_TABLE, HConstants.REPLICATION_SCOPE_GLOBAL,
      HConstants.REPLICATION_SCOPE_LOCAL);
    ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
      .setClusterKey("hbase+zk://localhost:2181").build();

    assertTrue(ReplicationExpectedTableUtil.isTableExpectedToReplicate(globalCf, peerConfig));

    TableDescriptor localOnly =
      table(USER_TABLE, HConstants.REPLICATION_SCOPE_LOCAL, HConstants.REPLICATION_SCOPE_LOCAL);
    assertFalse(ReplicationExpectedTableUtil.isTableExpectedToReplicate(localOnly, peerConfig));

    Map<TableName, List<String>> exclude = new HashMap<>();
    exclude.put(USER_TABLE, Collections.emptyList());
    ReplicationPeerConfig excludedPeer = ReplicationPeerConfig.newBuilder()
      .setClusterKey("hbase+zk://localhost:2181").setReplicateAllUserTables(true)
      .setExcludeTableCFsMap(exclude).build();
    assertFalse(ReplicationExpectedTableUtil.isTableExpectedToReplicate(globalCf, excludedPeer));
  }

  @Test
  public void testGetExpectedTablesSkipsSystemTables() {
    TableDescriptor userTable = table(USER_TABLE, HConstants.REPLICATION_SCOPE_GLOBAL);
    TableDescriptor metaTable = TableDescriptorBuilder.newBuilder(TableName.META_TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF_GLOBAL)
        .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
      .build();
    ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
      .setClusterKey("hbase+zk://localhost:2181").build();

    Set<TableName> expected = ReplicationExpectedTableUtil
      .getExpectedTables(List.of(userTable, metaTable), peerConfig);
    assertEquals(1, expected.size());
    assertTrue(expected.contains(USER_TABLE));
  }

  private static TableDescriptor table(TableName tableName, int... scopes) {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    for (int i = 0; i < scopes.length; i++) {
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(new byte[] { (byte) ('a' + i) })
        .setScope(scopes[i]).build());
    }
    return builder.build();
  }
}
