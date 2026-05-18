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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.Uninterruptibles;

@Tag(MediumTests.TAG)
public class TestBackupManager {

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupManager.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  protected static Configuration conf = UTIL.getConfiguration();
  protected static SingleProcessHBaseCluster cluster;
  protected static Connection conn;
  protected BackupManager backupManager;

  @BeforeAll
  public static void setUp() throws Exception {
    conf.setBoolean(BackupRestoreConstants.BACKUP_ENABLE_KEY, true);
    BackupManager.decorateMasterConfiguration(conf);
    BackupManager.decorateRegionServerConfiguration(conf);
    cluster = UTIL.startMiniCluster();
    conn = UTIL.getConnection();
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @BeforeEach
  public void before() throws IOException {
    backupManager = new BackupManager(conn, conn.getConfiguration());
  }

  @AfterEach
  public void after() {
    backupManager.close();
  }

  AtomicLongArray startTimes = new AtomicLongArray(2);
  AtomicLongArray stopTimes = new AtomicLongArray(2);

  @Test
  public void testStartBackupExclusiveOperation() {

    long sleepTime = 2000;
    Runnable r = new Runnable() {
      @Override
      public void run() {
        try {
          backupManager.startBackupSession();
          boolean result = startTimes.compareAndSet(0, 0, EnvironmentEdgeManager.currentTime());
          if (!result) {
            result = startTimes.compareAndSet(1, 0, EnvironmentEdgeManager.currentTime());
            if (!result) {
              throw new IOException("PANIC! Unreachable code");
            }
          }
          Thread.sleep(sleepTime);
          result = stopTimes.compareAndSet(0, 0, EnvironmentEdgeManager.currentTime());
          if (!result) {
            result = stopTimes.compareAndSet(1, 0, EnvironmentEdgeManager.currentTime());
            if (!result) {
              throw new IOException("PANIC! Unreachable code");
            }
          }
          backupManager.finishBackupSession();
        } catch (IOException | InterruptedException e) {
          fail("Unexpected exception: " + e.getMessage());
        }
      }
    };

    Thread[] workers = new Thread[2];
    for (int i = 0; i < workers.length; i++) {
      workers[i] = new Thread(r);
      workers[i].start();
    }

    for (int i = 0; i < workers.length; i++) {
      Uninterruptibles.joinUninterruptibly(workers[i]);
    }
    LOG.info("Diff start time=" + (startTimes.get(1) - startTimes.get(0)) + "ms");
    LOG.info("Diff finish time=" + (stopTimes.get(1) - stopTimes.get(0)) + "ms");
    assertTrue(startTimes.get(1) - startTimes.get(0) >= sleepTime);
    assertTrue(stopTimes.get(1) - stopTimes.get(0) >= sleepTime);

  }

}
