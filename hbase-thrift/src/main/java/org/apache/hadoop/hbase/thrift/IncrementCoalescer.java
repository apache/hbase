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

package org.apache.hadoop.hbase.thrift;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.thrift.ThriftServerRunner.HBaseHandler;
import org.apache.hadoop.hbase.thrift.generated.TIncrement;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.thrift.TException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will coalesce increments from a thift server if
 * hbase.regionserver.thrift.coalesceIncrement is set to true. Turning this
 * config to true will cause the thrift server to queue increments into an
 * instance of this class. The thread pool associated with this class will drain
 * the coalesced increments as the thread is able. This can cause data loss if the
 * thrift server dies or is shut down before everything in the queue is drained.
 *
 */
@InterfaceAudience.Private
public class IncrementCoalescer implements IncrementCoalescerMBean {

  /**
   * Used to identify a cell that will be incremented.
   *
   */
  static class FullyQualifiedRow {
    private byte[] table;
    private byte[] rowKey;
    private byte[] family;
    private byte[] qualifier;

    public FullyQualifiedRow(byte[] table, byte[] rowKey, byte[] fam, byte[] qual) {
      super();
      this.table = table;
      this.rowKey = rowKey;
      this.family = fam;
      this.qualifier = qual;
    }

    public byte[] getTable() {
      return table;
    }

    public void setTable(byte[] table) {
      this.table = table;
    }

    public byte[] getRowKey() {
      return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
      this.rowKey = rowKey;
    }

    public byte[] getFamily() {
      return family;
    }

    public void setFamily(byte[] fam) {
      this.family = fam;
    }

    public byte[] getQualifier() {
      return qualifier;
    }

    public void setQualifier(byte[] qual) {
      this.qualifier = qual;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(family);
      result = prime * result + Arrays.hashCode(qualifier);
      result = prime * result + Arrays.hashCode(rowKey);
      result = prime * result + Arrays.hashCode(table);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      FullyQualifiedRow other = (FullyQualifiedRow) obj;
      if (!Arrays.equals(family, other.family)) return false;
      if (!Arrays.equals(qualifier, other.qualifier)) return false;
      if (!Arrays.equals(rowKey, other.rowKey)) return false;
      if (!Arrays.equals(table, other.table)) return false;
      return true;
    }

  }

  static class DaemonThreadFactory implements ThreadFactory {
    static final AtomicInteger poolNumber = new AtomicInteger(1);
    final ThreadGroup group;
    final AtomicInteger threadNumber = new AtomicInteger(1);
    final String namePrefix;

    DaemonThreadFactory() {
      SecurityManager s = System.getSecurityManager();
      group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
      namePrefix = "ICV-" + poolNumber.getAndIncrement() + "-thread-";
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
      if (!t.isDaemon()) t.setDaemon(true);
      if (t.getPriority() != Thread.NORM_PRIORITY) t.setPriority(Thread.NORM_PRIORITY);
      return t;
    }
  }

  private final LongAdder failedIncrements = new LongAdder();
  private final LongAdder successfulCoalescings = new LongAdder();
  private final LongAdder totalIncrements = new LongAdder();
  private final ConcurrentMap<FullyQualifiedRow, Long> countersMap =
      new ConcurrentHashMap<>(100000, 0.75f, 1500);
  private final ThreadPoolExecutor pool;
  private final HBaseHandler handler;

  private int maxQueueSize = 500000;
  private static final int CORE_POOL_SIZE = 1;

  private static final Logger LOG = LoggerFactory.getLogger(FullyQualifiedRow.class);

  @SuppressWarnings("deprecation")
  public IncrementCoalescer(HBaseHandler hand) {
    this.handler = hand;
    LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    pool =
        new ThreadPoolExecutor(CORE_POOL_SIZE, CORE_POOL_SIZE, 50, TimeUnit.MILLISECONDS, queue,
            Threads.newDaemonThreadFactory("IncrementCoalescer"));

    MBeans.register("thrift", "Thrift", this);
  }

  public boolean queueIncrement(TIncrement inc) throws TException {
    if (!canQueue()) {
      failedIncrements.increment();
      return false;
    }
    return internalQueueTincrement(inc);
  }

  public boolean queueIncrements(List<TIncrement> incs) throws TException {
    if (!canQueue()) {
      failedIncrements.increment();
      return false;
    }

    for (TIncrement tinc : incs) {
      internalQueueTincrement(tinc);
    }
    return true;

  }

  private boolean internalQueueTincrement(TIncrement inc) throws TException {
    byte[][] famAndQf = CellUtil.parseColumn(inc.getColumn());
    if (famAndQf.length != 2) return false;

    return internalQueueIncrement(inc.getTable(), inc.getRow(), famAndQf[0], famAndQf[1],
      inc.getAmmount());
  }

  private boolean internalQueueIncrement(byte[] tableName, byte[] rowKey, byte[] fam,
      byte[] qual, long ammount) throws TException {
    int countersMapSize = countersMap.size();


    //Make sure that the number of threads is scaled.
    dynamicallySetCoreSize(countersMapSize);

    totalIncrements.increment();

    FullyQualifiedRow key = new FullyQualifiedRow(tableName, rowKey, fam, qual);

    long currentAmount = ammount;
    // Spin until able to insert the value back without collisions
    while (true) {
      Long value = countersMap.remove(key);
      if (value == null) {
        // There was nothing there, create a new value
        value = Long.valueOf(currentAmount);
      } else {
        value += currentAmount;
        successfulCoalescings.increment();
      }
      // Try to put the value, only if there was none
      Long oldValue = countersMap.putIfAbsent(key, value);
      if (oldValue == null) {
        // We were able to put it in, we're done
        break;
      }
      // Someone else was able to put a value in, so let's remember our
      // current value (plus what we picked up) and retry to add it in
      currentAmount = value;
    }

    // We limit the size of the queue simply because all we need is a
    // notification that something needs to be incremented. No need
    // for millions of callables that mean the same thing.
    if (pool.getQueue().size() <= 1000) {
      // queue it up
      Callable<Integer> callable = createIncCallable();
      pool.submit(callable);
    }

    return true;
  }

  public boolean canQueue() {
    return countersMap.size() < maxQueueSize;
  }

  private Callable<Integer> createIncCallable() {
    return new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        int failures = 0;
        Set<FullyQualifiedRow> keys = countersMap.keySet();
        for (FullyQualifiedRow row : keys) {
          Long counter = countersMap.remove(row);
          if (counter == null) {
            continue;
          }
          Table table = null;
          try {
            table = handler.getTable(row.getTable());
            if (failures > 2) {
              throw new IOException("Auto-Fail rest of ICVs");
            }
            table.incrementColumnValue(row.getRowKey(), row.getFamily(), row.getQualifier(),
              counter);
          } catch (IOException e) {
            // log failure of increment
            failures++;
            LOG.error("FAILED_ICV: " + Bytes.toString(row.getTable()) + ", "
                + Bytes.toStringBinary(row.getRowKey()) + ", "
                + Bytes.toStringBinary(row.getFamily()) + ", "
                + Bytes.toStringBinary(row.getQualifier()) + ", " + counter, e);
          } finally{
            if(table != null){
              table.close();
            }
          }
        }
        return failures;
      }
    };
  }

  /**
   * This method samples the incoming requests and, if selected, will check if
   * the corePoolSize should be changed.
   * @param countersMapSize
   */
  private void dynamicallySetCoreSize(int countersMapSize) {
    // Here we are using countersMapSize as a random number, meaning this
    // could be a Random object
    if (countersMapSize % 10 != 0) {
      return;
    }
    double currentRatio = (double) countersMapSize / (double) maxQueueSize;
    int newValue = 1;
    if (currentRatio < 0.1) {
      // it's 1
    } else if (currentRatio < 0.3) {
      newValue = 2;
    } else if (currentRatio < 0.5) {
      newValue = 4;
    } else if (currentRatio < 0.7) {
      newValue = 8;
    } else if (currentRatio < 0.9) {
      newValue = 14;
    } else {
      newValue = 22;
    }
    if (pool.getCorePoolSize() != newValue) {
      pool.setCorePoolSize(newValue);
    }
  }

  // MBean get/set methods
  @Override
  public int getQueueSize() {
    return pool.getQueue().size();
  }

  @Override
  public int getMaxQueueSize() {
    return this.maxQueueSize;
  }

  @Override
  public void setMaxQueueSize(int newSize) {
    this.maxQueueSize = newSize;
  }

  @Override
  public long getPoolCompletedTaskCount() {
    return pool.getCompletedTaskCount();
  }

  @Override
  public long getPoolTaskCount() {
    return pool.getTaskCount();
  }

  @Override
  public int getPoolLargestPoolSize() {
    return pool.getLargestPoolSize();
  }

  @Override
  public int getCorePoolSize() {
    return pool.getCorePoolSize();
  }

  @Override
  public void setCorePoolSize(int newCoreSize) {
    pool.setCorePoolSize(newCoreSize);
  }

  @Override
  public int getMaxPoolSize() {
    return pool.getMaximumPoolSize();
  }

  @Override
  public void setMaxPoolSize(int newMaxSize) {
    pool.setMaximumPoolSize(newMaxSize);
  }

  @Override
  public long getFailedIncrements() {
    return failedIncrements.sum();
  }

  @Override
  public long getSuccessfulCoalescings() {
    return successfulCoalescings.sum();
  }

  @Override
  public long getTotalIncrements() {
    return totalIncrements.sum();
  }

  @Override
  public long getCountersMapSize() {
    return countersMap.size();
  }

}
