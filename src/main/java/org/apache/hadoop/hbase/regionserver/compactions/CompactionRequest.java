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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
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
public class CompactionRequest implements Comparable<CompactionRequest>,
    Runnable {
    static final Log LOG = LogFactory.getLog(CompactionRequest.class);
    private final HRegion r;
    private final Store s;
    private CompactSelection compactSelection;
    private long totalSize;
    private boolean isMajor;
    private int p;
    private final Long timeInNanos;
    private HRegionServer server = null;

    /**
     * Map to track the number of compaction requested per region (id)
     */
    private static final ConcurrentHashMap<Long, AtomicInteger>
      majorCompactions = new ConcurrentHashMap<Long, AtomicInteger>();
    private static final ConcurrentHashMap<Long, AtomicInteger>
      minorCompactions = new ConcurrentHashMap<Long, AtomicInteger>();

  /**
   * Create a simple compaction request just for testing - this lets you specify everything you
   * would need in the general case of testing compactions from an external perspective (e.g.
   * requesting a compaction through the HRegion).
   * @param store
   * @param conf
   * @param selection
   * @param isMajor
   * @return a request that is useful in requesting compactions for testing
   */
  public static CompactionRequest getRequestForTesting(Store store, Configuration conf,
      Collection<StoreFile> selection, boolean isMajor) {
    return new CompactionRequest(store.getHRegion(), store, new CompactSelection(conf,
        new ArrayList<StoreFile>(
        selection)), isMajor, 0, System.nanoTime());
  }

  /**
   * Constructor for a custom compaction. Uses the setXXX methods to update the state of the
   * compaction before being used. Uses the current system time on creation as the start time.
   * @param region region that is being compacted
   * @param store store which is being compacted
   * @param priority specified priority with which this compaction should enter the queue.
   */
  public CompactionRequest(HRegion region, Store store, int priority) {
    this(region, store, null, false, priority, System.nanoTime());
  }

  public CompactionRequest(HRegion r, Store s, CompactSelection files, boolean isMajor, int p) {
    // delegate to the internal constructor after checking basic preconditions
    this(Preconditions.checkNotNull(r), s, Preconditions.checkNotNull(files), isMajor, p, System
        .nanoTime());
  }

  private CompactionRequest(HRegion region, Store store, CompactSelection files, boolean isMajor,
      int priority, long startTime) {
    this.r = region;
    this.s = store;
    this.isMajor = isMajor;
    this.p = priority;
    this.timeInNanos = startTime;
    if (files != null) {
      this.setSelection(files);
    }
  }

    /**
     * Find out if a given region in compaction now.
     *
     * @param regionId
     * @return
     */
    public static CompactionState getCompactionState(
        final long regionId) {
      Long key = Long.valueOf(regionId);
      AtomicInteger major = majorCompactions.get(key);
      AtomicInteger minor = minorCompactions.get(key);
      int state = 0;
      if (minor != null && minor.get() > 0) {
        state += 1;  // use 1 to indicate minor here
      }
      if (major != null && major.get() > 0) {
        state += 2;  // use 2 to indicate major here
      }
      switch (state) {
      case 3:  // 3 = 2 + 1, so both major and minor
        return CompactionState.MAJOR_AND_MINOR;
      case 2:
        return CompactionState.MAJOR;
      case 1:
        return CompactionState.MINOR;
      default:
        return CompactionState.NONE;
      }
    }

    public static void preRequest(final CompactionRequest cr){
      Long key = Long.valueOf(cr.getHRegion().getRegionId());
      ConcurrentHashMap<Long, AtomicInteger> compactions =
        cr.isMajor() ? majorCompactions : minorCompactions;
      AtomicInteger count = compactions.get(key);
      if (count == null) {
        compactions.putIfAbsent(key, new AtomicInteger(0));
        count = compactions.get(key);
      }
      count.incrementAndGet();
    }

    public static void postRequest(final CompactionRequest cr){
      Long key = Long.valueOf(cr.getHRegion().getRegionId());
      ConcurrentHashMap<Long, AtomicInteger> compactions =
        cr.isMajor() ? majorCompactions : minorCompactions;
      AtomicInteger count = compactions.get(key);
      if (count != null) {
        count.decrementAndGet();
      }
    }

    public void finishRequest() {
      this.compactSelection.finishRequest();
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

      compareVal = p - request.p; //compare priority
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

    /** Gets the HRegion for the request */
    public HRegion getHRegion() {
      return r;
    }

    /** Gets the Store for the request */
    public Store getStore() {
      return s;
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
      return p;
    }

    /** Gets the priority for the request */
    public void setPriority(int p) {
      this.p = p;
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

      return "regionName=" + r.getRegionNameAsString() +
        ", storeName=" + new String(s.getFamily().getName()) +
        ", fileCount=" + compactSelection.getFilesToCompact().size() +
        ", fileSize=" + StringUtils.humanReadableInt(totalSize) +
          ((fsList.isEmpty()) ? "" : " (" + fsList + ")") +
        ", priority=" + p + ", time=" + timeInNanos;
    }

    @Override
    public void run() {
      Preconditions.checkNotNull(server);
      if (server.isStopped()) {
        return;
      }
      try {
        long start = EnvironmentEdgeManager.currentTimeMillis();
        boolean completed = r.compact(this);
        long now = EnvironmentEdgeManager.currentTimeMillis();
        LOG.info(((completed) ? "completed" : "aborted") + " compaction: " +
              this + "; duration=" + StringUtils.formatTimeDiff(now, start));
        if (completed) {
          server.getMetrics().addCompaction(now - start, this.totalSize);
          // degenerate case: blocked regions require recursive enqueues
          if (s.getCompactPriority() <= 0) {
            server.getCompactSplitThread()
              .requestCompaction(r, s, "Recursive enqueue", null);
          } else {
            // see if the compaction has caused us to exceed max region size
            server.getCompactSplitThread().requestSplit(r);
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
        s.finishRequest(this);
      LOG.debug("CompactSplitThread status: " + server.getCompactSplitThread());
      }
    }

    /**
     * An enum for the region compaction state
     */
    public static enum CompactionState {
      NONE,
      MINOR,
      MAJOR,
      MAJOR_AND_MINOR;
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
