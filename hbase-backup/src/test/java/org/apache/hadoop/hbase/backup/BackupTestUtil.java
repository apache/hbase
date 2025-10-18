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
package org.apache.hadoop.hbase.backup;

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONTINUOUS_BACKUP_REPLICATION_PEER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class BackupTestUtil {
  private BackupTestUtil() {
  }

  static BackupInfo verifyBackup(Configuration conf, String backupId, BackupType expectedType,
    BackupInfo.BackupState expectedState) throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(conf);
      BackupAdmin backupAdmin = new BackupAdminImpl(connection)) {
      BackupInfo backupInfo = backupAdmin.getBackupInfo(backupId);

      // Verify managed backup in HBase
      assertEquals(backupId, backupInfo.getBackupId());
      assertEquals(expectedState, backupInfo.getState());
      assertEquals(expectedType, backupInfo.getType());
      return backupInfo;
    }
  }

  public static void enableBackup(Configuration conf) {
    // Enable backup
    conf.setBoolean(BackupRestoreConstants.BACKUP_ENABLE_KEY, true);
    BackupManager.decorateMasterConfiguration(conf);
    BackupManager.decorateRegionServerConfiguration(conf);
  }

  public static void verifyReplicationPeerSubscription(HBaseTestingUtil util, TableName tableName) throws IOException {
    try (Admin admin = util.getAdmin()) {
      ReplicationPeerDescription peerDesc = admin.listReplicationPeers().stream()
        .filter(peer -> peer.getPeerId().equals(CONTINUOUS_BACKUP_REPLICATION_PEER)).findFirst()
        .orElseThrow(() -> new AssertionError("Replication peer not found"));

      assertTrue("Table should be subscribed to the replication peer",
        peerDesc.getPeerConfig().getTableCFsMap().containsKey(tableName));
    }
  }
}
