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
package org.apache.hadoop.hbase.security.visibility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class VisibilityReplicationEndpoint implements ReplicationEndpoint {

  private static final Logger LOG = LoggerFactory.getLogger(VisibilityReplicationEndpoint.class);

  private final ReplicationEndpoint delegator;
  private final VisibilityLabelService visibilityLabelsService;

  public VisibilityReplicationEndpoint(ReplicationEndpoint endpoint,
    VisibilityLabelService visibilityLabelsService) {
    this.delegator = endpoint;
    this.visibilityLabelsService = visibilityLabelsService;
  }

  @Override
  public void init(Context context) throws IOException {
    delegator.init(context);
  }

  @Override
  public void peerConfigUpdated(ReplicationPeerConfig rpc) {
    delegator.peerConfigUpdated(rpc);
  }

  @Override
  public boolean replicate(ReplicateContext replicateContext) {
    if (!delegator.canReplicateToSameCluster()) {
      // Only when the replication is inter cluster replication we need to
      // convert the visibility tags to
      // string based tags. But for intra cluster replication like region
      // replicas it is not needed.
      List<Entry> entries = replicateContext.getEntries();
      List<Tag> visTags = new ArrayList<>();
      List<Tag> nonVisTags = new ArrayList<>();
      List<Entry> newEntries = new ArrayList<>(entries.size());
      for (Entry entry : entries) {
        WALEdit newEdit = new WALEdit();
        ArrayList<Cell> cells = entry.getEdit().getCells();
        for (Cell cell : cells) {
          if (cell.getTagsLength() > 0) {
            visTags.clear();
            nonVisTags.clear();
            Byte serializationFormat =
              VisibilityUtils.extractAndPartitionTags(cell, visTags, nonVisTags);
            if (!visTags.isEmpty()) {
              try {
                byte[] modifiedVisExpression = visibilityLabelsService
                  .encodeVisibilityForReplication(visTags, serializationFormat);
                if (modifiedVisExpression != null) {
                  nonVisTags
                    .add(new ArrayBackedTag(TagType.STRING_VIS_TAG_TYPE, modifiedVisExpression));
                }
              } catch (Exception ioe) {
                LOG.error(
                  "Exception while reading the visibility labels from the cell. The replication "
                    + "would happen as per the existing format and not as "
                    + "string type for the cell " + cell + ".",
                  ioe);
                // just return the old entries as it is without applying the string type change
                newEdit.add(cell);
                continue;
              }
              // Recreate the cell with the new tags and the existing tags
              Cell newCell = PrivateCellUtil.createCell(cell, nonVisTags);
              newEdit.add(newCell);
            } else {
              newEdit.add(cell);
            }
          } else {
            newEdit.add(cell);
          }
        }
        newEntries.add(new Entry((entry.getKey()), newEdit));
      }
      replicateContext.setEntries(newEntries);
      return delegator.replicate(replicateContext);
    } else {
      return delegator.replicate(replicateContext);
    }
  }

  @Override
  public synchronized UUID getPeerUUID() {
    return delegator.getPeerUUID();
  }

  @Override
  public boolean canReplicateToSameCluster() {
    return delegator.canReplicateToSameCluster();
  }

  @Override
  public WALEntryFilter getWALEntryfilter() {
    return delegator.getWALEntryfilter();
  }

  @Override
  public boolean isRunning() {
    return this.delegator.isRunning();
  }

  @Override
  public boolean isStarting() {
    return this.delegator.isStarting();
  }

  @Override
  public void start() {
    this.delegator.start();
  }

  @Override
  public void awaitRunning() {
    this.delegator.awaitRunning();
  }

  @Override
  public void awaitRunning(long timeout, TimeUnit unit) throws TimeoutException {
    this.delegator.awaitRunning(timeout, unit);
  }

  @Override
  public void stop() {
    this.delegator.stop();
  }

  @Override
  public void awaitTerminated() {
    this.delegator.awaitTerminated();
  }

  @Override
  public void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException {
    this.delegator.awaitTerminated(timeout, unit);
  }

  @Override
  public Throwable failureCause() {
    return this.delegator.failureCause();
  }
}
