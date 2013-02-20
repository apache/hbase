/**
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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

/**
 * This class holds all details necessary to run a compaction.
 */
@InterfaceAudience.LimitedPrivate({ "coprocessor" })
@InterfaceStability.Evolving
public class CompactionRequest implements Comparable<CompactionRequest>,
    Runnable {
    static final Log LOG = LogFactory.getLog(CompactionRequest.class);
    private final HRegion region;
    private final HStore store;
    private CompactSelection compactSelection;
    private long totalSize;
    private boolean isMajor;
    private int priority;
    private final Long timeInNanos;
    private HRegionServer server = null;

    public static CompactionRequest getRequestForTesting(Collection<StoreFile> selection,
        boolean isMajor) {
      return new CompactionRequest(null, null, new CompactSelection(new ArrayList<StoreFile>(
        selection)), isMajor, 0, System.nanoTime());
    }

    /**
     * Constructor for a custom compaction. Uses the setXXX methods to update the state of the
     * compaction before being used.
     */
    public CompactionRequest(HRegion region, HStore store, int priority) {
    this(region, store, null, false, priority, System
        .nanoTime());
    }

    public CompactionRequest(HRegion r, HStore s, CompactSelection files, boolean isMajor, int p) {
      // delegate to the internal constructor after checking basic preconditions
      this(Preconditions.checkNotNull(r), s, Preconditions.checkNotNull(files), isMajor, p, System
          .nanoTime());
    }

    private CompactionRequest(HRegion region, HStore store, CompactSelection files, boolean isMajor,
        int priority, long startTime) {
      this.region = region;
      this.store = store;
      this.isMajor = isMajor;
      this.priority = priority;
      this.timeInNanos = startTime;
      if (files != null) {
        this.setSelection(files);
      }
    }

    /**
     * This function will define where in the priority queue the request will
     * end up.  Those with the highest priorities will be first.  When the
     * priorities are the same it will first compare priority then date
     * to maintain a FIFO functionality.
     *
     * <p>Note: The date is only accurate to the millisecond which means it is
     * possible that two requests were inserted into the queue within a
     * millisecond.  When that is the case this function will break the tie
     * arbitrarily.
     */
    @Override
    public int compareTo(CompactionRequest request) {
      //NOTE: The head of the priority queue is the least element
      if (this.equals(request)) {
        return 0; //they are the same request
      }
      int compareVal;

      compareVal = priority - request.priority; //compare priority
      if (compareVal != 0) {
        return compareVal;
      }

      compareVal = timeInNanos.compareTo(request.timeInNanos);
      if (compareVal != 0) {
        return compareVal;
      }

      // break the tie based on hash code
      return this.hashCode() - request.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return (this == obj);
    }

    /** Gets the HRegion for the request */
    public HRegion getHRegion() {
      return region;
    }

    /** Gets the Store for the request */
    public HStore getStore() {
      return store;
    }

    /** Gets the compact selection object for the request */
    public CompactSelection getCompactSelection() {
      return compactSelection;
    }

    /** Gets the StoreFiles for the request */
    public List<StoreFile> getFiles() {
      return compactSelection.getFilesToCompact();
    }

    /** Gets the total size of all StoreFiles in compaction */
    public long getSize() {
      return totalSize;
    }

    public boolean isMajor() {
      return this.isMajor;
    }

    /** Gets the priority for the request */
    public int getPriority() {
      return priority;
    }

    public long getSelectionTime() {
      return compactSelection.getSelectionTime();
    }

    /** Gets the priority for the request */
    public void setPriority(int p) {
      this.priority = p;
    }

    public void setServer(HRegionServer hrs) {
      this.server = hrs;
    }

    /**
     * Set the files (and, implicitly, the size of the compaction based on those files)
     * @param files files that should be included in the compaction
     */
    public void setSelection(CompactSelection files) {
      long sz = 0;
      for (StoreFile sf : files.getFilesToCompact()) {
        sz += sf.getReader().length();
      }
      this.totalSize = sz;
      this.compactSelection = files;
    }

    /**
     * Specify if this compaction should be a major compaction based on the state of the store
     * @param isMajor <tt>true</tt> if the system determines that this compaction should be a major
     *          compaction
     */
    public void setIsMajor(boolean isMajor) {
      this.isMajor = isMajor;
    }

    @Override
    public String toString() {
      String fsList = Joiner.on(", ").join(
          Collections2.transform(Collections2.filter(
              compactSelection.getFilesToCompact(),
              new Predicate<StoreFile>() {
                public boolean apply(StoreFile sf) {
                  return sf.getReader() != null;
                }
            }), new Function<StoreFile, String>() {
              public String apply(StoreFile sf) {
                return StringUtils.humanReadableInt(sf.getReader().length());
              }
            }));

      return "regionName=" + region.getRegionNameAsString() +
        ", storeName=" + new String(store.getFamily().getName()) +
        ", fileCount=" + compactSelection.getFilesToCompact().size() +
        ", fileSize=" + StringUtils.humanReadableInt(totalSize) +
          ((fsList.isEmpty()) ? "" : " (" + fsList + ")") +
        ", priority=" + priority + ", time=" + timeInNanos;
    }

    @Override
    public void run() {
      Preconditions.checkNotNull(server);
      if (server.isStopped()) {
        return;
      }
      try {
        long start = EnvironmentEdgeManager.currentTimeMillis();
        boolean completed = region.compact(this);
        long now = EnvironmentEdgeManager.currentTimeMillis();
        LOG.info(((completed) ? "completed" : "aborted") + " compaction: " +
              this + "; duration=" + StringUtils.formatTimeDiff(now, start));
        if (completed) {
          // degenerate case: blocked regions require recursive enqueues
          if (store.getCompactPriority() <= 0) {
            server.compactSplitThread.requestCompaction(region, store, "Recursive enqueue", null);
            } else {
              // see if the compaction has caused us to exceed max region size
          server.getCompactSplitThread().requestSplit(region);
            }
        }
      } catch (IOException ex) {
        LOG.error("Compaction failed " + this, RemoteExceptionHandler
            .checkIOException(ex));
        server.checkFileSystem();
      } catch (Exception ex) {
        LOG.error("Compaction failed " + this, ex);
        server.checkFileSystem();
      } finally {
        store.finishRequest(this);
        LOG.debug("CompactSplitThread Status: " + server.compactSplitThread);
      }
    }

    /**
     * Cleanup class to use when rejecting a compaction request from the queue.
     */
    public static class Rejection implements RejectedExecutionHandler {

      @Override
      public void rejectedExecution(Runnable request, ThreadPoolExecutor pool) {
        if (request instanceof CompactionRequest) {
          CompactionRequest cr = (CompactionRequest) request;
          LOG.debug("Compaction Rejected: " + cr);
          cr.getStore().finishRequest(cr);
        }
      }
    }
}
