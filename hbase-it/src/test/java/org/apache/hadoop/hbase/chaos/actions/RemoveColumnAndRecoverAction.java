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
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounter.RetryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Action that removes a column family and then restores the table to the state prior to that action
 * using a recovery snapshot, if one is available. If a recovery snapshot is not available it will
 * log a warning and take no further action. After recovering from the found recovery snapshot it
 * will delete the recovery snapshot.
 */
public class RemoveColumnAndRecoverAction extends RemoveColumnAction {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveColumnAndRecoverAction.class);
  protected Pattern snapshotNamePattern;

  public RemoveColumnAndRecoverAction(TableName tableName, Set<String> protectedColumns) {
    super(tableName, protectedColumns);
    snapshotNamePattern = Pattern.compile("auto_" + tableName.getNameAsString() + "_[0-9]+");
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws Exception {
    // First perform the remove column action
    super.perform();

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
            DeleteAndRecoverTableAction.restoreTable(context, tableName, snapshot);
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

}
