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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.LockTimeoutException;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.filecompactions.MobFileCompactor;
import org.apache.hadoop.hbase.mob.filecompactions.PartitionedMobFileCompactor;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.Threads;

/**
 * The Class MobFileCompactChore for running compaction regularly to merge small mob files.
 */
@InterfaceAudience.Private
public class MobFileCompactionChore extends ScheduledChore {

  private static final Log LOG = LogFactory.getLog(MobFileCompactionChore.class);
  private HMaster master;
  private TableLockManager tableLockManager;
  private ExecutorService pool;

  public MobFileCompactionChore(HMaster master) {
    super(master.getServerName() + "-MobFileCompactChore", master,
        master.getConfiguration().getInt(MobConstants.MOB_FILE_COMPACTION_CHORE_PERIOD,
      MobConstants.DEFAULT_MOB_FILE_COMPACTION_CHORE_PERIOD));
    this.master = master;
    this.tableLockManager = master.getTableLockManager();
    this.pool = createThreadPool();
  }

  @Override
  protected void chore() {
    try {
      String className = master.getConfiguration().get(MobConstants.MOB_FILE_COMPACTOR_CLASS_KEY,
        PartitionedMobFileCompactor.class.getName());
      TableDescriptors htds = master.getTableDescriptors();
      Map<String, HTableDescriptor> map = htds.getAll();
      for (HTableDescriptor htd : map.values()) {
        for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
          if (!hcd.isMobEnabled()) {
            continue;
          }
          // instantiate the mob file compactor.
          MobFileCompactor compactor = null;
          try {
            compactor = ReflectionUtils.instantiateWithCustomCtor(className, new Class[] {
              Configuration.class, FileSystem.class, TableName.class, HColumnDescriptor.class,
              ExecutorService.class },
              new Object[] { master.getConfiguration(), master.getFileSystem(), htd.getTableName(),
                hcd, pool });
          } catch (Exception e) {
            throw new IOException("Unable to load configured mob file compactor '" + className
              + "'", e);
          }
          // compact only for mob-enabled column.
          // obtain a write table lock before performing compaction to avoid race condition
          // with major compaction in mob-enabled column.
          boolean tableLocked = false;
          TableLock lock = null;
          try {
            // the tableLockManager might be null in testing. In that case, it is lock-free.
            if (tableLockManager != null) {
              lock = tableLockManager.writeLock(MobUtils.getTableLockName(htd.getTableName()),
                "Run MobFileCompactChore");
              lock.acquire();
            }
            tableLocked = true;
            compactor.compact();
          } catch (LockTimeoutException e) {
            LOG.info("Fail to acquire the lock because of timeout, maybe a major compaction or an"
              + " ExpiredMobFileCleanerChore is running", e);
          } catch (Exception e) {
            LOG.error("Fail to compact the mob files for the column " + hcd.getNameAsString()
              + " in the table " + htd.getNameAsString(), e);
          } finally {
            if (lock != null && tableLocked) {
              try {
                lock.release();
              } catch (IOException e) {
                LOG.error(
                  "Fail to release the write lock for the table " + htd.getNameAsString(), e);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Fail to clean the expired mob files", e);
    }
  }

  @Override
  protected void cleanup() {
    super.cleanup();
    pool.shutdown();
  }

  /**
   * Creates a thread pool.
   * @return A thread pool.
   */
  private ExecutorService createThreadPool() {
    Configuration conf = master.getConfiguration();
    int maxThreads = conf.getInt(MobConstants.MOB_FILE_COMPACTION_CHORE_THREADS_MAX,
      MobConstants.DEFAULT_MOB_FILE_COMPACTION_CHORE_THREADS_MAX);
    if (maxThreads == 0) {
      maxThreads = 1;
    }
    long keepAliveTime = conf.getLong(MobConstants.MOB_FILE_COMPACTION_CHORE_THREADS_KEEPALIVETIME,
      MobConstants.DEFAULT_MOB_FILE_COMPACTION_CHORE_THREADS_KEEPALIVETIME);
    final SynchronousQueue<Runnable> queue = new SynchronousQueue<Runnable>();
    ThreadPoolExecutor pool = new ThreadPoolExecutor(1, maxThreads, keepAliveTime,
      TimeUnit.SECONDS, queue, Threads.newDaemonThreadFactory("MobFileCompactionChore"),
      new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
          try {
            // waiting for a thread to pick up instead of throwing exceptions.
            queue.put(r);
          } catch (InterruptedException e) {
            throw new RejectedExecutionException(e);
          }
        }
    });
    ((ThreadPoolExecutor) pool).allowCoreThreadTimeOut(true);
    return pool;
  }
}
