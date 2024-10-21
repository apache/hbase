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
package org.apache.hadoop.hbase.chaos.actions;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounter.RetryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Action that drops a table and then recovers it from the recovery snapshot, if available. If a
 * recovery snapshot is not available it will recreate the table instead and log a warning. After
 * recovering from the found recovery snapshot it will delete the recovery snapshot.
 */
public class DeleteAndRecoverTableAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteAndRecoverTableAction.class);
  private final TableName tableName;
  protected Pattern snapshotNamePattern;

  public DeleteAndRecoverTableAction(TableName tableName) {
    this.tableName = tableName;
    snapshotNamePattern = Pattern.compile("auto_" + tableName.getNameAsString() + "_[0-9]+");
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws Exception {
    HBaseTestingUtil util = context.getHBaseIntegrationTestingUtility();
    Admin admin = util.getAdmin();
    TableDescriptor descriptor = admin.getDescriptor(tableName);

    // Drop and recreate the table
    admin.disableTable(tableName);
    try {
      try {
        admin.deleteTable(tableName);
      } finally {
        // We may need to retry this a few times because of ambient chaos.
        RetryCounter retryCounter = new RetryCounter(new RetryConfig().setMaxAttempts(10));
        while (true) {
          try {
            if (!admin.tableExists(tableName)) {
              admin.createTable(descriptor);
            }
            break;
          } catch (IOException e) {
            if (retryCounter.shouldRetry()) {
              getLogger().warn("Retrying recreation of " + tableName, e);
              retryCounter.sleepUntilNextRetry();
            } else {
              getLogger().error("Recreate of " + tableName + " failed after too many retries!", e);
              break;
            }
          }
        }
      }
    } finally {
      if (admin.isTableDisabled(tableName)) {
        try {
          admin.enableTable(tableName);
        } catch (TableNotDisabledException e) {
          // Ignore
        }
      }
    }

    // Now lets try to recover from that action
    List<SnapshotDescription> snapshots = admin.listSnapshots(snapshotNamePattern);
    if (!snapshots.isEmpty()) {
      // It's possible due to various event sequences under chaos that we have more than one
      // recovery snapshot for the table even though we are trying to clean up. The most
      // recent snapshot is almost certainly ours.
      snapshots.sort(new Comparator<SnapshotDescription>() {
        @Override
        public int compare(SnapshotDescription o1, SnapshotDescription o2) {
          // Reversed order by name, so we get the most recent
          return o2.getName().compareTo(o1.getName());
        }
      });
      SnapshotDescription snapshot = snapshots.get(0);
      try {
        // We might need to retry this because of ambient chaos
        RetryCounter retryCounter = new RetryCounter(new RetryConfig().setMaxAttempts(10));
        while (true) {
          try {
            restoreTable(context, tableName, snapshot);
            break;
          } catch (IOException e) {
            if (retryCounter.shouldRetry()) {
              getLogger().warn("Retrying restore of " + tableName + " from " + snapshot.getName()
                + " after exception", e);
              retryCounter.sleepUntilNextRetry();
            } else {
              throw e;
            }
          }
        }
      } catch (IOException e) {
        getLogger().warn("Failed to restore " + tableName + " from " + snapshot.getName(), e);
      } finally {
        try {
          admin.deleteSnapshot(snapshot.getName());
        } catch (IOException e) {
          getLogger().warn("Failed to delete recovery snapshot " + snapshot.getName());
        }
      }
    } else {
      getLogger().warn("No recovery snapshots found for " + tableName);
    }
  }

  static void restoreTable(ActionContext context, TableName tableName, SnapshotDescription snapshot)
    throws IOException {
    Admin admin = context.getHBaseIntegrationTestingUtility().getAdmin();
    try {
      if (admin.isTableEnabled(tableName)) {
        try {
          admin.disableTable(tableName);
        } catch (TableNotEnabledException e) {
          // Ignore
        }
      }
      admin.restoreSnapshot(snapshot.getName());
    } finally {
      if (admin.isTableDisabled(tableName)) {
        try {
          admin.enableTable(tableName);
        } catch (TableNotDisabledException e) {
          // Ignore
        }
      }
    }
  }

}
