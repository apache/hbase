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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * The mob file compaction thread used in {@link MasterRpcServices}
 */
@InterfaceAudience.Private
public class MasterMobFileCompactionThread {
  static final Log LOG = LogFactory.getLog(MasterMobFileCompactionThread.class);
  private final HMaster master;
  private final Configuration conf;
  private final ExecutorService mobFileCompactorPool;
  private final ExecutorService masterMobPool;

  public MasterMobFileCompactionThread(HMaster master) {
    this.master = master;
    this.conf = master.getConfiguration();
    final String n = Thread.currentThread().getName();
    // this pool is used to run the mob file compaction
    this.masterMobPool = new ThreadPoolExecutor(1, 2, 60, TimeUnit.SECONDS,
      new SynchronousQueue<Runnable>(), new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r);
          t.setName(n + "-MasterMobFileCompaction-" + EnvironmentEdgeManager.currentTime());
          return t;
        }
      });
    ((ThreadPoolExecutor) this.masterMobPool).allowCoreThreadTimeOut(true);
    // this pool is used in the mob file compaction to compact the mob files by partitions
    // in parallel
    this.mobFileCompactorPool = MobUtils
      .createMobFileCompactorThreadPool(master.getConfiguration());
  }

  /**
   * Requests mob file compaction
   * @param conf The Configuration
   * @param fs The file system
   * @param tableName The table the compact
   * @param hcds The column descriptors
   * @param tableLockManager The tableLock manager
   * @param isForceAllFiles Whether add all mob files into the compaction.
   */
  public void requestMobFileCompaction(Configuration conf, FileSystem fs, TableName tableName,
    List<HColumnDescriptor> hcds, TableLockManager tableLockManager, boolean isForceAllFiles)
    throws IOException {
    master.reportMobFileCompactionStart(tableName);
    try {
      masterMobPool.execute(new CompactionRunner(fs, tableName, hcds, tableLockManager,
        isForceAllFiles, mobFileCompactorPool));
    } catch (RejectedExecutionException e) {
      // in case the request is rejected by the pool
      try {
        master.reportMobFileCompactionEnd(tableName);
      } catch (IOException e1) {
        LOG.error("Failed to mark end of mob file compation", e1);
      }
      throw e;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("The mob file compaction is requested for the columns " + hcds + " of the table "
        + tableName.getNameAsString());
    }
  }

  private class CompactionRunner implements Runnable {
    private FileSystem fs;
    private TableName tableName;
    private List<HColumnDescriptor> hcds;
    private TableLockManager tableLockManager;
    private boolean isForceAllFiles;
    private ExecutorService pool;

    public CompactionRunner(FileSystem fs, TableName tableName, List<HColumnDescriptor> hcds,
      TableLockManager tableLockManager, boolean isForceAllFiles, ExecutorService pool) {
      super();
      this.fs = fs;
      this.tableName = tableName;
      this.hcds = hcds;
      this.tableLockManager = tableLockManager;
      this.isForceAllFiles = isForceAllFiles;
      this.pool = pool;
    }

    @Override
    public void run() {
      try {
        for (HColumnDescriptor hcd : hcds) {
          MobUtils.doMobFileCompaction(conf, fs, tableName, hcd, pool, tableLockManager,
            isForceAllFiles);
        }
      } catch (IOException e) {
        LOG.error("Failed to perform the mob file compaction", e);
      } finally {
        try {
          master.reportMobFileCompactionEnd(tableName);
        } catch (IOException e) {
          LOG.error("Failed to mark end of mob file compation", e);
        }
      }
    }
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  private void interruptIfNecessary() {
    mobFileCompactorPool.shutdown();
    masterMobPool.shutdown();
  }

  /**
   * Wait for all the threads finish.
   */
  private void join() {
    waitFor(mobFileCompactorPool, "Mob file Compaction Thread");
    waitFor(masterMobPool, "Region Server Mob File Compaction Thread");
  }

  /**
   * Closes the MasterMobFileCompactionThread.
   */
  public void close() {
    interruptIfNecessary();
    join();
  }

  /**
   * Wait for thread finish.
   * @param t the thread to wait
   * @param name the thread name.
   */
  private void waitFor(ExecutorService t, String name) {
    boolean done = false;
    while (!done) {
      try {
        done = t.awaitTermination(60, TimeUnit.SECONDS);
        LOG.info("Waiting for " + name + " to finish...");
        if (!done) {
          t.shutdownNow();
        }
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted waiting for " + name + " to finish...");
      }
    }
  }
}
