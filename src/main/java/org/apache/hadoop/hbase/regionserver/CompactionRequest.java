/**
 * Copyright 2011 The Apache Software Foundation
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

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
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
    private final CompactSelection compactSelection;
    private final long totalSize;
    private final boolean isMajor;
    private int p;
    private final Date date;
    private HRegionServer server = null;    

    CompactionRequest(HRegion r, Store s,
        CompactSelection files, boolean isMajor, int p) {
      Preconditions.checkNotNull(r);
      Preconditions.checkNotNull(files);

      this.r = r;
      this.s = s;
      this.compactSelection = files;
      long sz = 0;
      for (StoreFile sf : files.getFilesToCompact()) {
        sz += sf.getReader().length();
      }
      this.totalSize = sz;
      this.isMajor = isMajor;
      this.p = p;
      this.date = new Date();
    }

    long getCompactSelectionID() {
      return compactSelection.getCompactSelectionID();
    }

    long getSelectionTime() {
      return compactSelection.getSelectionTime();
    }

    void finishRequest() {
      this.compactSelection.finishRequest();
    }

    /**
     * This function will define where in the priority queue the request will
     * end up.  Those with the highest priorities will be first.  When the
     * priorities are the same it will It will first compare priority then date
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

      compareVal = date.compareTo(request.date);
      if (compareVal != 0) {
        return compareVal;
      }

      // break the tie based on hash code
      return this.hashCode() - request.hashCode();
    }

    /** Gets the HRegion for the request */
    HRegion getHRegion() {
      return r;
    }
    
    /** Gets the Store for the request */
    Store getStore() {
      return s;
    }

    /** Gets the compact selection object for the request */
    CompactSelection getCompactSelection() {
      return compactSelection;
    }
    
    /** Gets the StoreFiles for the request */
    List<StoreFile> getFiles() {
      return compactSelection.getFilesToCompact();
    }
    
    /** Gets the total size of all StoreFiles in compaction */
    long getSize() {
      return totalSize;
    }
    
    boolean isMajor() {
      return this.isMajor;
    }

    void setServer(HRegionServer server) {
      this.server = server;
    }

    /** Gets the priority for the request */
    int getPriority() {
      return p;
    }

    /** Gets the priority for the request */
    void setPriority(int p) {
      this.p = p;
    }

  @Override
    public String toString() {
      String fsList = Joiner.on(", ").join(
        Collections2.transform(Collections2.filter(compactSelection.getFilesToCompact(),
            new Predicate<StoreFile>() {
              public boolean apply(StoreFile sf) {
                return sf.getReader() != null;
              }
            }),
        new Function<StoreFile, String>() {
          public String apply(StoreFile sf) {
            
            return StringUtils.humanReadableInt(sf.getReader().length());
          }
        }));

      return "regionName=" + r.getRegionNameAsString() +
        ", storeName=" + new String(s.getFamily().getName()) +
        ", fileCount=" + compactSelection.getFilesToCompact().size() + 
        ", fileSize=" + StringUtils.humanReadableInt(totalSize) +
          ((fsList.isEmpty()) ? "" : " (" + fsList + ")") +
        ", priority=" + p + ", date=" + date;
    }

    @Override
    public void run() {
      try {
        if (server.isStopRequested()) {
          return;
        } 
        long start = EnvironmentEdgeManager.currentTimeMillis();
        boolean completed = r.compact(this);
        long now = EnvironmentEdgeManager.currentTimeMillis();
        LOG.debug(((completed) ? "completed" : "aborted") + " compaction: " +
            this + "; duration=" + StringUtils.formatTimeDiff(now, start));
        if (completed) {
          server.getMetrics().addCompaction(now - start, this.totalSize);
          // degenerate case: blocked regions require recursive enqueues
          if (s.getCompactPriority() <= 0) {
            server.compactSplitThread
              .requestCompaction(r, s, "Recursive enqueue");
          }
        } 
      } catch (IOException ex) {
        LOG.error("Compaction failed " + this, RemoteExceptionHandler
            .checkIOException(ex));
        if (server != null) {
          server.checkFileSystem();
        }
      } catch (Exception ex) {
        LOG.error("Compaction failed " + this, ex);
        if (server != null) {
          server.checkFileSystem();
        }
      } finally {
        s.finishRequest(this);
        if (server != null) {
          LOG.debug("CompactSplitThread Status: " + server.compactSplitThread);
        }
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
