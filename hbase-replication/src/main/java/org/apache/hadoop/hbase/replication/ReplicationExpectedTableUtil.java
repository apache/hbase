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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Determines which source-cluster tables are expected to exist on a replication peer target,
 * using the same peer-config and {@link HConstants#REPLICATION_SCOPE_GLOBAL} rules applied when
 * shipping WAL edits.
 */
@InterfaceAudience.Private
public final class ReplicationExpectedTableUtil {

  private ReplicationExpectedTableUtil() {
  }

  /**
   * Returns true when the table has at least one column family that would be replicated to the
   * given peer (global replication scope and included by peer configuration).
   */
  public static boolean isTableExpectedToReplicate(TableDescriptor tableDescriptor,
    ReplicationPeerConfig peerConfig) {
    TableName tableName = tableDescriptor.getTableName();
    if (tableName.isSystemTable()) {
      return false;
    }
    if (!peerConfig.needToReplicate(tableName)) {
      return false;
    }
    for (ColumnFamilyDescriptor columnFamily : tableDescriptor.getColumnFamilies()) {
      if (columnFamily.getScope() == HConstants.REPLICATION_SCOPE_GLOBAL
        && peerConfig.needToReplicate(tableName, columnFamily.getName())) {
        return true;
      }
    }
    return false;
  }

  /** Returns source tables that should exist on the peer target cluster. */
  public static Set<TableName> getExpectedTables(Collection<TableDescriptor> tableDescriptors,
    ReplicationPeerConfig peerConfig) {
    Set<TableName> expected = new HashSet<>();
    for (TableDescriptor tableDescriptor : tableDescriptors) {
      if (isTableExpectedToReplicate(tableDescriptor, peerConfig)) {
        expected.add(tableDescriptor.getTableName());
      }
    }
    return expected;
  }

  /** Returns the subset of {@code expectedTables} that do not exist on the peer cluster. */
  public static Set<TableName> getMissingTargetTables(Set<TableName> expectedTables,
    Configuration peerClusterConf) throws IOException {
    Set<TableName> missing = new HashSet<>();
    try (Connection connection = ConnectionFactory.createConnection(peerClusterConf);
      Admin admin = connection.getAdmin()) {
      for (TableName tableName : expectedTables) {
        if (!admin.tableExists(tableName)) {
          missing.add(tableName);
        }
      }
    }
    return missing;
  }

  public static int countMissingTargetTables(Set<TableName> expectedTables,
    Configuration peerClusterConf) throws IOException {
    return getMissingTargetTables(expectedTables, peerClusterConf).size();
  }
}
