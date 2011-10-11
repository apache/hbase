/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Compact region on request and then run split if appropriate
 */
class CompactSplitThread extends Thread {
  static final Log LOG = LogFactory.getLog(CompactSplitThread.class);

  private HTable root = null;
  private HTable meta = null;
  private final long frequency;
  private final ReentrantLock lock = new ReentrantLock();

  private final HRegionServer server;
  private final Configuration conf;

  private final PriorityCompactionQueue compactionQueue =
    new PriorityCompactionQueue();

  /* The default priority for user-specified compaction requests.
   * The user gets top priority unless we have blocking compactions (Pri <= 0)
   */
  public static final int PRIORITY_USER = 1;

  /** @param server */
  public CompactSplitThread(HRegionServer server) {
    super();
    this.server = server;
    this.conf = server.conf;
    this.frequency =
      conf.getLong("hbase.regionserver.thread.splitcompactcheckfrequency",
      20 * 1000);
  }

  @Override
  public void run() {
    while (!this.server.isStopRequested()) {
      CompactionRequest compactionRequest = null;
      HRegion r = null;
      try {
        compactionRequest = compactionQueue.poll(this.frequency, TimeUnit.MILLISECONDS);
        if (compactionRequest != null) {
          lock.lock();
          try {
            if(!this.server.isStopRequested()) {
              // Don't interrupt us while we are working
              r = compactionRequest.getHRegion();
              byte [] midKey = r.compactStore(compactionRequest.getStore());
              if (r.getLastCompactInfo() != null) {  // compaction aborted?
                this.server.getMetrics().addCompaction(r.getLastCompactInfo());
              }
              if (LOG.isDebugEnabled()) {
                CompactionRequest next = this.compactionQueue.peek();
                LOG.debug("Just finished a compaction. " +
                          " Current Compaction Queue: size=" +
                          getCompactionQueueSize() +
                          ((next != null) ?
                              ", topPri=" + next.getPriority() : ""));
              }
              if (!this.server.isStopRequested()) {
                // requests that were added during compaction will have a
                // stale priority. remove and re-insert to update priority
                boolean hadCompaction = compactionQueue.remove(compactionRequest);
                if (midKey != null) {
                  split(r, midKey);
                } else if (hadCompaction) {
                  // recompute the priority for a request already in the queue
                  LOG.debug("Re-computing priority for compaction request " + compactionRequest);
                  compactionRequest.setPriority(compactionRequest.getStore().getCompactPriority());
                  compactionQueue.add(compactionRequest);
                } else if (compactionRequest.getStore().getCompactPriority() < PRIORITY_USER) {
                  // degenerate case. recursively enqueue blocked regions
                  LOG.debug("Re-queueing with " + compactionRequest.getStore().getCompactPriority() +
                      " priority for compaction request " + compactionRequest);
                  compactionRequest.setPriority(compactionRequest.getStore().getCompactPriority());
                  compactionQueue.add(compactionRequest);
                }
              }
            }
          } finally {
            lock.unlock();
          }
        }
      } catch (InterruptedException ex) {
        continue;
      } catch (IOException ex) {
        LOG.error("Compaction/Split failed for region " +
            r.getRegionNameAsString(),
          RemoteExceptionHandler.checkIOException(ex));
        if (!server.checkFileSystem()) {
          break;
        }
      } catch (Exception ex) {
        LOG.error("Compaction failed" +
            (r != null ? (" for region " + r.getRegionNameAsString()) : ""),
            ex);
        if (!server.checkFileSystem()) {
          break;
        }
      }
    }
    compactionQueue.clear();
    LOG.info(getName() + " exiting");
  }

  /**
   * @param r HRegion store belongs to
   * @param why Why compaction requested -- used in debug messages
   */
  public synchronized void requestCompaction(final HRegion r,
      final String why) {
    for(Store s : r.getStores().values()) {
      requestCompaction(r, s, false, why, s.getCompactPriority());
    }
  }

  public synchronized void requestCompaction(final HRegion r,
      final String why, int p) {
    requestCompaction(r, false, why, p);
  }

  public synchronized void requestCompaction(final HRegion r,
      final boolean force, final String why, int p) {
    for(Store s : r.getStores().values()) {
      requestCompaction(r, s, force, why, p);
    }
  }

  /**
   * @param r HRegion store belongs to
   * @param force Whether next compaction should be major
   * @param why Why compaction requested -- used in debug messages
   */
  public synchronized void requestCompaction(final HRegion r, final Store s,
      final boolean force, final String why, int priority) {

    boolean addedToQueue = false;

    if (this.server.stopRequested.get()) {
      return;
    }

    // tell the region to major-compact (and don't downgrade it)
    if (force) {
      s.setForceMajorCompaction(force);
    }
    CompactionRequest compactionRequest = new CompactionRequest(r, s, priority);
    addedToQueue = compactionQueue.add(compactionRequest);
    // only log if actually added to compaction queue...
    if (addedToQueue && LOG.isDebugEnabled()) {
      LOG.debug("Compaction " + (force? "(major) ": "") +
        "requested for region " + r.getRegionNameAsString() +
        "/" + r.getRegionInfo().getEncodedName() +
        ", store " + s +
        (why != null && !why.isEmpty()? " because: " + why: "") +
        "; Priority: " + priority + "; Compaction queue size: " + compactionQueue.size());
    }
  }

  private void split(final HRegion region, final byte [] midKey)
  throws IOException {
    final HRegionInfo oldRegionInfo = region.getRegionInfo();
    final long startTime = System.currentTimeMillis();
    final HRegion[] newRegions = region.splitRegion(midKey);
    if (newRegions == null) {
      // Didn't need to be split
      return;
    }

    // When a region is split, the META table needs to updated if we're
    // splitting a 'normal' region, and the ROOT table needs to be
    // updated if we are splitting a META region.
    HTable t = null;
    if (region.getRegionInfo().isMetaTable()) {
      // We need to update the root region
      if (this.root == null) {
        this.root = new HTable(conf, HConstants.ROOT_TABLE_NAME);
      }
      t = root;
    } else {
      // For normal regions we need to update the meta region
      if (meta == null) {
        meta = new HTable(conf, HConstants.META_TABLE_NAME);
      }
      t = meta;
    }

    // Mark old region as offline and split in META.
    // NOTE: there is no need for retry logic here. HTable does it for us.
    oldRegionInfo.setOffline(true);
    oldRegionInfo.setSplit(true);
    // Inform the HRegionServer that the parent HRegion is no-longer online.
    this.server.removeFromOnlineRegions(oldRegionInfo);

    Put put = new Put(oldRegionInfo.getRegionName());
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
      Writables.getBytes(oldRegionInfo));
    put.add(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
        HConstants.EMPTY_BYTE_ARRAY);
    put.add(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
        HConstants.EMPTY_BYTE_ARRAY);
    put.add(HConstants.CATALOG_FAMILY, HConstants.SPLITA_QUALIFIER,
      Writables.getBytes(newRegions[0].getRegionInfo()));
    put.add(HConstants.CATALOG_FAMILY, HConstants.SPLITB_QUALIFIER,
      Writables.getBytes(newRegions[1].getRegionInfo()));
    t.put(put);

    // If we crash here, then the daughters will not be added and we'll have
    // and offlined parent but no daughters to take up the slack.  hbase-2244
    // adds fixup to the metascanners.

    // Add new regions to META
    for (int i = 0; i < newRegions.length; i++) {
      put = new Put(newRegions[i].getRegionName());
      put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
          Writables.getBytes(newRegions[i].getRegionInfo()));
      t.put(put);
    }

    // If we crash here, the master will not know of the new daughters and they
    // will not be assigned.  The metascanner when it runs will notice and take
    // care of assigning the new daughters.

    // Now tell the master about the new regions
    server.reportSplit(oldRegionInfo, newRegions[0].getRegionInfo(),
      newRegions[1].getRegionInfo());

    LOG.info("region split, META updated, and report to master all" +
      " successful. Old region=" + oldRegionInfo.toString() +
      ", new regions: " + newRegions[0].toString() + ", " +
      newRegions[1].toString() + ". Split took " +
      StringUtils.formatTimeDiff(System.currentTimeMillis(), startTime));
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  void interruptIfNecessary() {
    if (lock.tryLock()) {
      try {
        this.interrupt();
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * Returns the current size of the queue containing regions that are
   * processed.
   *
   * @return The current size of the regions queue.
   */
  public int getCompactionQueueSize() {
    return compactionQueue.size();
  }
}
