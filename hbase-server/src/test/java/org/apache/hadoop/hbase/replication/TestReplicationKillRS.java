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

import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TestReplicationKillRS extends TestReplicationBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationKillRS.class);

  /**
   * Load up 1 tables over 2 region servers and kill a source during the upload. The failover
   * happens internally. WARNING this test sometimes fails because of HBASE-3515
   */
  protected void loadTableAndKillRS(HBaseTestingUtility util) throws Exception {
    // killing the RS with hbase:meta can result into failed puts until we solve
    // IO fencing
    int rsToKill1 = util.getHBaseCluster().getServerWithMeta() == 0 ? 1 : 0;

    // Takes about 20 secs to run the full loading, kill around the middle
    Thread killer = killARegionServer(util, 5000, rsToKill1);
    Result[] res;
    int initialCount;
    try (Connection conn = ConnectionFactory.createConnection(CONF1)) {
      try (Table table = conn.getTable(tableName)) {
        LOG.info("Start loading table");
        initialCount = UTIL1.loadTable(table, famName);
        LOG.info("Done loading table");
        killer.join(5000);
        LOG.info("Done waiting for threads");

        while (true) {
          try (ResultScanner scanner = table.getScanner(new Scan())) {
            res = scanner.next(initialCount);
            break;
          } catch (UnknownScannerException ex) {
            LOG.info("Cluster wasn't ready yet, restarting scanner");
          }
        }
      }
    }
    // Test we actually have all the rows, we may miss some because we
    // don't have IO fencing.
    if (res.length != initialCount) {
      LOG.warn("We lost some rows on the master cluster!");
      // We don't really expect the other cluster to have more rows
      initialCount = res.length;
    }

    int lastCount = 0;
    final long start = EnvironmentEdgeManager.currentTime();
    int i = 0;
    try (Connection conn = ConnectionFactory.createConnection(CONF2)) {
      try (Table table = conn.getTable(tableName)) {
        while (true) {
          if (i == NB_RETRIES - 1) {
            fail("Waited too much time for queueFailover replication. " + "Waited "
                + (EnvironmentEdgeManager.currentTime() - start) + "ms.");
          }
          Result[] res2;
          try (ResultScanner scanner = table.getScanner(new Scan())) {
            res2 = scanner.next(initialCount * 2);
          }
          if (res2.length < initialCount) {
            if (lastCount < res2.length) {
              i--; // Don't increment timeout if we make progress
            } else {
              i++;
            }
            lastCount = res2.length;
            LOG.info(
              "Only got " + lastCount + " rows instead of " + initialCount + " current i=" + i);
            Thread.sleep(SLEEP_TIME * 2);
          } else {
            break;
          }
        }
      }
    }
  }

  private static Thread killARegionServer(final HBaseTestingUtility utility, final long timeout,
      final int rs) {
    Thread killer = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(timeout);
          utility.getHBaseCluster().getRegionServer(rs).stop("Stopping as part of the test");
        } catch (Exception e) {
          LOG.error("Couldn't kill a region server", e);
        }
      }
    };
    killer.setDaemon(true);
    killer.start();
    return killer;
  }
}
