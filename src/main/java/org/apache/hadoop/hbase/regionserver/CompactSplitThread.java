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
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
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

  protected final BlockingQueue<CompactionRequest> compactionQueue =
    new PriorityBlockingQueue<CompactionRequest>();

  /* The default priority for user-specified compaction requests.
   * The user gets top priority unless we have blocking compactions (Pri <= 0)
   */
  public static final int PRIORITY_USER = 1;
  public static final int NO_PRIORITY = Integer.MIN_VALUE;

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
      boolean completed = false;
      try {
        compactionRequest = compactionQueue.poll(this.frequency, TimeUnit.MILLISECONDS);
        if (compactionRequest != null) {
          r = compactionRequest.getHRegion();
          lock.lock();
          try {
            // look for a split first
            if(!this.server.isStopRequested()) {
              // don't split regions that are blocking
              if (r.getCompactPriority() >= PRIORITY_USER) {
                byte[] midkey = compactionRequest.getStore().checkSplit();
                if (midkey != null) {
                  split(r, midkey);
                  continue;
                }
              }
            }

            // now test for compaction
            if (!this.server.isStopRequested()) {
              long startTime = EnvironmentEdgeManager.currentTimeMillis();
              completed = r.compact(compactionRequest);
              long now = EnvironmentEdgeManager.currentTimeMillis();
              LOG.info(((completed) ? "completed" : "aborted")
                  + " compaction: " + compactionRequest + ", duration="
                  + StringUtils.formatTimeDiff(now, startTime));
              if (completed) { // compaction aborted?
                this.server.getMetrics().
                  addCompaction(now - startTime, compactionRequest.getSize());
              }
              if (LOG.isDebugEnabled()) {
                CompactionRequest next = this.compactionQueue.peek();
                LOG.debug("Just finished a compaction. " +
                          " Current Compaction Queue: size=" +
                          getCompactionQueueSize() +
                          ((next != null) ?
                              ", topPri=" + next.getPriority() : ""));
              }
            }
          } finally {
            lock.unlock();
          }
        }
      } catch (InterruptedException ex) {
        continue;
      } catch (IOException ex) {
        LOG.error("Compaction/Split failed " + compactionRequest,
          RemoteExceptionHandler.checkIOException(ex));
        if (!server.checkFileSystem()) {
          break;
        }
      } catch (Exception ex) {
        LOG.error("Compaction failed " + compactionRequest, ex);
        if (!server.checkFileSystem()) {
          break;
        }
      } finally {
        if (compactionRequest != null) {
          Store s = compactionRequest.getStore();
          s.finishRequest(compactionRequest);
          // degenerate case: blocked regions require recursive enqueues
          if (s.getCompactPriority() < PRIORITY_USER && completed) {
            requestCompaction(r, s, "Recursive enqueue");
          }
        }
        compactionRequest = null;
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
      requestCompaction(r, s, why, NO_PRIORITY);
    }
  }

  public synchronized void requestCompaction(final HRegion r, final Store s,
      final String why) {
    requestCompaction(r, s, why, NO_PRIORITY);
  }

  public synchronized void requestCompaction(final HRegion r, final String why,
      int p) {
    for(Store s : r.getStores().values()) {
      requestCompaction(r, s, why, p);
    }
  }

  /**
   * @param r HRegion store belongs to
   * @param force Whether next compaction should be major
   * @param why Why compaction requested -- used in debug messages
   * @param priority override the default priority (NO_PRIORITY == decide)
   */
  public synchronized void requestCompaction(final HRegion r, final Store s,
      final String why, int priority) {

    boolean addedToQueue = false;

    if (this.server.stopRequested.get()) {
      return;
    }

    CompactionRequest cr = s.requestCompaction();
    if (cr != null) {
      if (priority != NO_PRIORITY) {
        cr.setPriority(priority);
      }
      addedToQueue = compactionQueue.add(cr);
      if (!addedToQueue) {
        LOG.error("Could not add request to compaction queue: " + cr);
        s.finishRequest(cr);
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("Compaction requested: " + cr
            + (why != null && !why.isEmpty() ? "; Because: " + why : "")
            + "; Priority: " + priority + "; Compaction queue size: "
            + compactionQueue.size());
      }
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
