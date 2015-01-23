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
package org.apache.hadoop.hbase.regionserver;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.util.Threads;

import com.google.common.annotations.VisibleForTesting;

/**
 * Manages heap memory related tasks.
 */
@InterfaceAudience.Private
public class HeapMemoryManager {
  private static final Log LOG = LogFactory.getLog(HeapMemoryManager.class);

  // keep the same period tunable as branch-1 and higher for compatibility
  public static final String HBASE_RS_HEAP_MEMORY_TUNER_PERIOD = 
      "hbase.regionserver.heapmemory.tuner.period";
  public static final int HBASE_RS_HEAP_MEMORY_TUNER_DEFAULT_PERIOD = 60 * 1000;

  private float heapOccupancyPercent;

  private Server server;
  private HeapMemoryChore heapMemChore = null;
  private final int defaultChorePeriod;
  private final float heapOccupancyLowWatermark;

  public static HeapMemoryManager create(Server server) {
    return new HeapMemoryManager(server);
  }

  @VisibleForTesting
  HeapMemoryManager(Server server) {
    Configuration conf = server.getConfiguration();
    this.server = server;
    this.defaultChorePeriod = conf.getInt(HBASE_RS_HEAP_MEMORY_TUNER_PERIOD,
      HBASE_RS_HEAP_MEMORY_TUNER_DEFAULT_PERIOD);
    this.heapOccupancyLowWatermark = conf.getFloat(HConstants.HEAP_OCCUPANCY_LOW_WATERMARK_KEY,
      HConstants.DEFAULT_HEAP_OCCUPANCY_LOW_WATERMARK);
  }

  public void start() {
    this.heapMemChore = new HeapMemoryChore();
    Threads.setDaemonThreadRunning(heapMemChore.getThread());
  }

  public void stop() {
    // The thread is Daemon. Just interrupting the ongoing process.
    this.heapMemChore.interrupt();
  }

  /**
   * @return heap occupancy percentage, 0 <= n <= 1
   */
  public float getHeapOccupancyPercent() {
    return this.heapOccupancyPercent;
  }

  private class HeapMemoryChore extends Chore {
    private boolean alarming = false;

    public HeapMemoryChore() {
      super(server.getServerName() + "-HeapMemoryChore", defaultChorePeriod, server);
    }

    @Override
    protected void sleep() {
      if (!alarming) {
        super.sleep();
      } else {
        // we are in the alarm state, so sleep only for a short fixed period
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // Interrupted, propagate
          Thread.currentThread().interrupt();
        }
      }
    }

    @Override
    protected void chore() {
      // Sample heap occupancy
      MemoryUsage memUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
      heapOccupancyPercent = (float)memUsage.getUsed() / (float)memUsage.getCommitted();
      // If we are above the heap occupancy alarm low watermark, sound the alarm
      if (heapOccupancyPercent >= heapOccupancyLowWatermark) {
        if (!alarming) {
          LOG.warn("heapOccupancyPercent " + heapOccupancyPercent +
            " is above heap occupancy alarm watermark (" + heapOccupancyLowWatermark + ")");
          alarming = true;
        }
      } else {
        if (alarming) {
          LOG.info("heapOccupancyPercent " + heapOccupancyPercent +
            " is now below the heap occupancy alarm watermark (" +
            heapOccupancyLowWatermark + ")");
          alarming = false;
        }
      }
    }
  }
}
