/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.AsyncTableRegionLocator;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link org.apache.hadoop.hbase.replication.ReplicationEndpoint} endpoint which receives the WAL
 * edits from the WAL, and sends the edits to replicas of regions.
 * This EndPoint is specially for system tables. It does a few things:
 * 1. For each replica region, there is a shipper to ship edits for this region. Internally,
 * the shipper maintains a in-memory queue, where edits from wal log are queued in this
 * in-memory queue.
 * 2. Each shipper will read edits from its queue and ship edits to the specific replica region.
 * The pace of shippers are independent from each other, so they are not synced.
 * 3. In case of flush event COMMIT_FLUSH, edits in the slow shipper will be discarded as
 * those edits are already in flushed hfiles. This also prevents the queue grow out-of-bound
 * when the replica region server is slow processing edits.
 * 4. register/deregister region is based on region events from wal log.
 * 5. CatalogReplicaReplicationEndpoint is based on BaseReplicationEndpoint as it does not need
 * zookeeper support.
 * TODO: the threading model for shippers needs to be improved. Right now, each shipper maps to
 * one thread, which is not efficient. It needs to be improved so two threads are needed.
 * One sending thread which loops through shipper's queue and send out the walEdits.
 * One receiving thread which will wait for futures, once it is ready, it processes that
 * future accordingly.
 */
@InterfaceAudience.Private public class CatalogReplicaReplicationEndpoint
  extends BaseReplicationEndpoint {

  private static final Logger LOG =
    LoggerFactory.getLogger(CatalogReplicaReplicationEndpoint.class);

  private Configuration conf;
  private AsyncClusterConnection connection;

  private int numRetries;
  private long operationTimeoutNs;
  private long shippedEdits;

  // Track primary regions under replication.
  ConcurrentHashMap<String, CatalogReplicaShipper[]> primaryRegionsUnderReplication;

  private byte[][] replicatedFamilies;

  public CatalogReplicaReplicationEndpoint() {
    // TODO: right now, it only allow family info to be replicated.
    // This need to be changed in the future.
    replicatedFamilies = new byte[][] { HConstants.CATALOG_FAMILY };
    primaryRegionsUnderReplication = new ConcurrentHashMap<>();
  }

  public long getShippedEdits() {
    return shippedEdits;
  }

  @Override public void init(Context context) throws IOException {
    super.init(context);
    this.conf = context.getConfiguration();
    // HRS multiplies client retries by 10 globally for meta operations, but we do not want this.
    // We are resetting it here because we want default number of retries (35) rather than 10 times
    // that which makes very long retries for disabled tables etc.
    int defaultNumRetries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    if (defaultNumRetries > 10) {
      int mult = conf.getInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER,
        HConstants.DEFAULT_HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER);
      defaultNumRetries = defaultNumRetries / mult; // reset if HRS has multiplied this already
    }
    this.numRetries = conf.getInt(CLIENT_RETRIES_NUMBER, defaultNumRetries);
    // use the regular RPC timeout for replica replication RPC's
    this.operationTimeoutNs = TimeUnit.MILLISECONDS.toNanos(conf
      .getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT));
    this.connection = context.getServer().getAsyncClusterConnection();
  }

  /**
   * returns true if the specified entry must be replicated. We should always replicate meta
   * operations (e.g. flush) and use the configured family to filter out edits which do not
   * need to be replicated.
   */
  private boolean requiresReplication(final WALEdit edit) {

    // TODO: let meta edits go through. Need to filter out flush events which are not for
    // targetting families.
    if (edit.isMetaEdit()) {
      return true;
    }
    // empty edit does not need to be replicated
    if (edit.isEmpty()) {
      return false;
    }

    for (byte[] fam : replicatedFamilies) {
      if (edit.getFamilies().contains(fam)) {
        return true;
      }
    }

    return false;
  }

  // Register a new primary region to the endpoint when a region is opened, it will setup its
  // shipper to replicate its wal edits to replica regions.
  private void registerRegion(final String encodedName, final byte[] regionName)
    throws IOException {
    // Make sure it does not exist.
    LOG.info("Register region {}", encodedName);

    if (primaryRegionsUnderReplication.get(encodedName) != null) {
      LOG.warn("Try to register an existing regoin {}", encodedName);
      return;
    }

    RegionInfo regionInfo = CatalogFamilyFormat.parseRegionInfoFromRegionName(regionName);

    // TODO: meta replicas is not at the table descriptor at this moment.
    int numReplicas =
      conf.getInt(HConstants.META_REPLICAS_NUM, HConstants.DEFAULT_META_REPLICA_NUM);

    if (numReplicas <= 1) {
      LOG.warn("Trying to register region {} without replica enabled", encodedName);
      return;
    }

    CatalogReplicaShipper[] replicaShippers = new CatalogReplicaShipper[numReplicas - 1];

    for (int i = 1; i < numReplicas; i++) {
      // It leaves each ReplicaShipper to resolve its replica region location.
      replicaShippers[i - 1] = new CatalogReplicaShipper(this, this.connection, null,
        RegionInfoBuilder.newBuilder(regionInfo).setReplicaId(i).build());

      replicaShippers[i - 1].start();
    }

    primaryRegionsUnderReplication.put(encodedName, replicaShippers);
  }

  // Deregister a primary region from the endpoint when a region is closed, it will clean up all
  // resources allocated for this region.
  private void unregisterRegion(final String encodedName) {
    LOG.info("Unregister region {}", encodedName);
    // Make sure it does exist.
    if (primaryRegionsUnderReplication.get(encodedName) == null) {
      LOG.warn("Try to unregister a unregistered region {}", encodedName);
      return;
    }

    CatalogReplicaShipper[] replicaShippers = primaryRegionsUnderReplication.remove(encodedName);

    for (CatalogReplicaShipper s : replicaShippers) {
      s.stopShipper();
    }
  }

  @Override public boolean replicate(ReplicateContext replicateContext) {
    Map<String, List<List<Entry>>> encodedRegionName2Entries = new TreeMap<>();
    long skippedEdits = 0;

    // First, separate edits based on primary region.
    for (Entry entry : replicateContext.getEntries()) {
      WALEdit edit = entry.getEdit();
      if (!requiresReplication(edit)) {
        skippedEdits++;
        continue;
      }
      shippedEdits++;

      try {
        // If an edit is a meta regionevent edit, we need to check a few things.
        //    1. If it is a region_open event, it needs to register this region.
        //    2. If it is a region_close event, it needs to deregister this region.
        if (edit.isMetaEdit()) {
          WALProtos.RegionEventDescriptor regionEvent =
            WALEdit.getRegionEventDescriptor(edit.getCells().get(0));

          if (regionEvent != null) {
            if (regionEvent.getEventType()
              == WALProtos.RegionEventDescriptor.EventType.REGION_CLOSE) {
              unregisterRegion(Bytes.toString(entry.getKey().getEncodedRegionName()));
            } else if (regionEvent.getEventType()
              == WALProtos.RegionEventDescriptor.EventType.REGION_OPEN) {
              try {
                registerRegion(Bytes.toString(regionEvent.getEncodedRegionName().toByteArray()),
                  regionEvent.getRegionName().toByteArray());
              } catch (IOException e) {
                LOG.warn("REGION_OPEN, invalid region name {}", regionEvent.getRegionName());
              }
            }
          }
        }
        // add entry to the lists, please note one thing, if it is flush event, it needs to be
        // in its own list, it makes the cleanup of shipper's queue much easier.
        String encodeName = Bytes.toString(entry.getKey().getEncodedRegionName());
        List<List<Entry>> ll = encodedRegionName2Entries.get(encodeName);
        if (ll == null) {
          ll = new ArrayList<>();
          List<Entry> l = new ArrayList();
          l.add(entry);
          ll.add(l);
          encodedRegionName2Entries.put(encodeName, ll);
        } else {
          // Check if it can be merged into the last set.
          List<Entry> l = ll.get(ll.size() - 1);
          WALProtos.FlushDescriptor flushDesc =
            WALEdit.getFlushDescriptor(l.get(0).getEdit().getCells().get(0));
          if (flushDesc != null) {
            // Flush events need to be in their own list
            List<Entry> newList = new ArrayList<>();
            newList.add(entry);
            ll.add(newList);
          } else {
            // Check if the entry itself is a flush event.
            flushDesc = WALEdit.getFlushDescriptor(edit.getCells().get(0));
            if (flushDesc != null) {
              // Flush events need to be in their own list
              List<Entry> newList = new ArrayList<>();
              newList.add(entry);
              ll.add(newList);
            } else {
              l.add(entry);
            }
          }
        }
      } catch (IOException ioe) {
        LOG.warn("Failed to parse {}", entry.getEdit().getCells().get(0));
        skippedEdits++;
      }
    }

    // queue edits to shippers.

    for (Map.Entry<String, List<List<Entry>>> entry : encodedRegionName2Entries.entrySet()) {
      CatalogReplicaShipper[] replicaShippers = primaryRegionsUnderReplication.get(entry.getKey());
      if (replicaShippers != null) {
        for (List<Entry> l : entry.getValue()) {
          for (CatalogReplicaShipper shipper : replicaShippers) {
            shipper.addEditEntries(l);
          }
        }
      } else {
        skippedEdits++;
      }
    }
    ctx.getMetrics().incrLogEditsFiltered(skippedEdits);

    return true;
  }

  @Override public UUID getPeerUUID() {
    return this.ctx.getClusterId();
  }

  @Override public boolean canReplicateToSameCluster() {
    return true;
  }

  @Override protected WALEntryFilter getScopeWALEntryFilter() {
    // we do not care about scope. We replicate everything.
    return null;
  }

  @Override public void start() {
    startAsync();
  }

  @Override public void stop() {
    // Stop shippers
    for (Map.Entry<String, CatalogReplicaShipper[]> e : primaryRegionsUnderReplication.entrySet()) {
      for (CatalogReplicaShipper s : e.getValue()) {
        s.stopShipper();
      }
    }
    stopAsync();
  }

  @Override protected void doStart() {
    notifyStarted();
  }

  @Override protected void doStop() {
    notifyStopped();
  }

  public int getNumRetries() {
    return numRetries;
  }

  public long getOperationTimeoutNs() {
    return operationTimeoutNs;
  }

  private static class CatalogReplicaShipper extends Thread {
    private final CatalogReplicaReplicationEndpoint endpoint;
    private LinkedBlockingQueue<List<Entry>> queue;
    private final AsyncClusterConnection conn;
    //Indicates whether this particular worker is running
    private volatile boolean isRunning = true;
    private int numRetries;
    private HRegionLocation location = null;
    private RegionInfo regionInfo;
    private long operationTimeoutNs;
    private AsyncTableRegionLocator locator;

    CatalogReplicaShipper(final CatalogReplicaReplicationEndpoint endpoint,
      final AsyncClusterConnection connection, final HRegionLocation loc,
      final RegionInfo regionInfo) {
      this.endpoint = endpoint;
      this.conn = connection;
      this.location = loc;
      this.numRetries = endpoint.getNumRetries();
      this.regionInfo = regionInfo;
      this.queue = new LinkedBlockingQueue<>();
      this.operationTimeoutNs = endpoint.getOperationTimeoutNs();
      locator = conn.getRegionLocator(regionInfo.getTable());
    }

    private void cleanQueue(final long startFlushSeqOpId) {
      // remove entries with seq# less than startFlushSeqOpId

      // TODO: It assumes one family in the queue.
      // In the future, if more families are involved, it needs to have families into
      // consideration.
      queue.removeIf(n -> (n.get(0).getKey().getSequenceId() < startFlushSeqOpId));
    }

    public void addEditEntries(final List<Entry> entries) {
      try {
        if ((entries.size() == 1) && entries.get(0).getEdit().isMetaEdit()) {
          try {
            // When it is a COMMIT_FLUSH event, it can remove all edits in the queue whose
            // seq# is less flushSeq#. Those events include flush events and
            // OPEN_REGION/CLOSE_REGION events, as they work like flush events.
            WALProtos.FlushDescriptor flushDesc =
              WALEdit.getCommitFlushDescriptor(entries.get(0).getEdit().getCells().get(0));
            if (flushDesc != null) {
              cleanQueue(flushDesc.getFlushSequenceNumber());
            }
          } catch (IOException ioe) {
            LOG.warn("Failed to parse {}", entries.get(0).getEdit().getCells().get(0));
          }
        }
        queue.put(entries);
      } catch (InterruptedException e) {

      }
    }

    private CompletableFuture<Long> replicate(List<Entry> entries) {
      CompletableFuture<Long> future = new CompletableFuture<>();
      FutureUtils.addListener(conn
        .replay(regionInfo.getTable(), regionInfo.getEncodedNameAsBytes(), regionInfo.getStartKey(),
          entries, regionInfo.getReplicaId(), numRetries, operationTimeoutNs), (r, e) -> {
        if (e != null) {
          LOG.warn("Failed to replicate to {}, exception: ", regionInfo, e);
          future.completeExceptionally(e);
        } else {
          future.complete(r);
        }
      });

      return future;
    }

    @Override public final void run() {
      List<Entry> entries = null;
      while (isRunning()) {
        try {
          // If entries is not null, it has to resend the previous batch.
          if (entries == null) {
            entries = queue.poll(20000, TimeUnit.MILLISECONDS);
          }

          if (entries == null) {
            continue;
          }

          CompletableFuture<Long> future = replicate(entries);

          future.get();
          // successfully sent the batch.
          entries = null;
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          LOG.warn("Failed to replicate {} entries for region  of meta table, retry, ",
            entries.size(), cause);
        } catch (InterruptedException e) {
          // It is interrupted and needs to quit.
          LOG.warn("Interrupted while waiting for next replication entry batch", e);
          Thread.currentThread().interrupt();
        }
      }
    }

    /**
     * @return whether the thread is running
     */
    public boolean isRunning() {
      return isRunning && !isInterrupted();
    }

    /**
     * stop the shipper thread.
     */
    public void stopShipper() {
      this.isRunning = true;
    }
  }
}

