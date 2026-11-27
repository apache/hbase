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
package org.apache.hadoop.hbase.backup.impl;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.backup.BackupCopyJob;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupRestoreFactory;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(SmallTests.TAG)
public class TestIncrementalTableBackupClientCopyFailure {

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final Throwable COPY_ERROR =
    new IllegalArgumentException(new UnknownHostException("rm-0.example"));

  @Test
  public void testCopyFailureKeepsCopyJobErrorAsCause() {
    IncrementalTableBackupClient client = new IncrementalTableBackupClient();
    client.conf = new Configuration(TEST_UTIL.getConfiguration());
    client.conf.setClass(BackupRestoreFactory.HBASE_BACKUP_COPY_IMPL_CLASS, FailingCopyJob.class,
      BackupCopyJob.class);
    client.backupInfo = new BackupInfo();
    client.backupInfo.setBackupId("backup_1");
    client.backupInfo.setBackupRootDir(TEST_UTIL.getDataTestDir("backupRoot").toUri().toString());

    IOException e = assertThrows(IOException.class,
      () -> client.incrementalCopyHFiles(new String[] { "src" }, "dest"));

    assertSame(COPY_ERROR, e.getCause());
  }

  public static class FailingCopyJob implements BackupCopyJob {
    private Configuration conf;

    @Override
    public int copy(BackupInfo backupInfo, BackupManager backupManager, Configuration conf,
      BackupType copyType, String[] options) {
      return -999;
    }

    @Override
    public void cancel(String jobHandler) {
    }

    @Override
    public Optional<Throwable> getError() {
      return Optional.of(COPY_ERROR);
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return conf;
    }
  }
}
