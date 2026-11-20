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

import static org.apache.hadoop.hbase.backup.TestBackupUtils.assertBackupCompleteFileDoesNotExist;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.backup.impl.FullTableBackupClient;
import org.apache.hadoop.hbase.backup.impl.TableBackupClient;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(LargeTests.TAG)
public class TestFullBackupMarkerFailure extends TestBackupBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestFullBackupMarkerFailure.class);

  @Test
  public void testFullBackupSucceedsWhenMarkerWriteFails() throws Exception {
    // Use a special backup class that will force a failure when trying to write the
    // .backup.complete marker file
    conf1.set(TableBackupClient.BACKUP_CLIENT_IMPL_CLASS,
      FullTableBackupClientWithMarkerFailure.class.getName());

    try {
      String backupId = fullTableBackup(List.of(table1));

      // The backup should still succeed despite failure to write the .backup.complete marker file
      assertTrue(checkSucceeded(backupId));
      assertBackupCompleteFileDoesNotExist(conf1, BACKUP_ROOT_DIR, backupId);
    } finally {
      conf1.unset(TableBackupClient.BACKUP_CLIENT_IMPL_CLASS);
    }
  }

  // This class is used run the full backup. It is a special class that contains some overrides
  // that will force a failure when writing the .backup.complete marker file
  static class FullTableBackupClientWithMarkerFailure extends FullTableBackupClient {
    public FullTableBackupClientWithMarkerFailure() {
    }

    @Override
    protected void completeBackup(Connection conn, BackupInfo backupInfo, BackupType type,
      Configuration conf) throws IOException {
      Configuration markerFailConf = new Configuration(conf);
      markerFailConf.setClass("fs.hdfs.impl", MarkerCreationFailHDFS.class, FileSystem.class);
      markerFailConf.setBoolean("fs.hdfs.impl.disable.cache", true);
      super.completeBackup(conn, backupInfo, type, markerFailConf);
    }
  }

  public static class MarkerCreationFailHDFS extends FilterFileSystem {
    public MarkerCreationFailHDFS() {
      super(new DistributedFileSystem());
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
      if (f.getName().equals(HBackupFileSystem.BACKUP_COMPLETE_FILE_NAME)) {
        throw new IOException("Injected failure for marker creation test");
      }
      return super.create(f, overwrite);
    }
  }
}
