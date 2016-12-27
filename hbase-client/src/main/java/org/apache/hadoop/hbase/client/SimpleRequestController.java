/*
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
package org.apache.hadoop.hbase.client;

import com.google.common.annotations.VisibleForTesting;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;
import static org.apache.hadoop.hbase.util.CollectionUtils.computeIfAbsent;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Holds back the request if the submitted size or number has reached the
 * threshold.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class SimpleRequestController implements RequestController {
  private static final Log LOG = LogFactory.getLog(SimpleRequestController.class);
  /**
   * The maximum size of single RegionServer.
   */
  public static final String HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE = "hbase.client.max.perrequest.heapsize";

  /**
   * Default value of #HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE
   */
  @VisibleForTesting
  static final long DEFAULT_HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE = 4194304;

  /**
   * The maximum size of submit.
   */
  public static final String HBASE_CLIENT_MAX_SUBMIT_HEAPSIZE = "hbase.client.max.submit.heapsize";
  /**
   * Default value of #HBASE_CLIENT_MAX_SUBMIT_HEAPSIZE
   */
  @VisibleForTesting
  static final long DEFAULT_HBASE_CLIENT_MAX_SUBMIT_HEAPSIZE = DEFAULT_HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE;
  @VisibleForTesting
  final AtomicLong tasksInProgress = new AtomicLong(0);
  @VisibleForTesting
  final ConcurrentMap<byte[], AtomicInteger> taskCounterPerRegion
          = new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);
  @VisibleForTesting
  final ConcurrentMap<ServerName, AtomicInteger> taskCounterPerServer = new ConcurrentHashMap<>();
  /**
   * The number of tasks simultaneously executed on the cluster.
   */
  private final int maxTotalConcurrentTasks;

  /**
   * The max heap size of all tasks simultaneously executed on a server.
   */
  private final long maxHeapSizePerRequest;
  private final long maxHeapSizeSubmit;
  /**
   * The number of tasks we run in parallel on a single region. With 1 (the
   * default) , we ensure that the ordering of the queries is respected: we
   * don't start a set of operations on a region before the previous one is
   * done. As well, this limits the pressure we put on the region server.
   */
  @VisibleForTesting
  final int maxConcurrentTasksPerRegion;

  /**
   * The number of task simultaneously executed on a single region server.
   */
  @VisibleForTesting
  final int maxConcurrentTasksPerServer;
  private final int thresholdToLogUndoneTaskDetails;
  public static final String THRESHOLD_TO_LOG_UNDONE_TASK_DETAILS =
      "hbase.client.threshold.log.details";
  private static final int DEFAULT_THRESHOLD_TO_LOG_UNDONE_TASK_DETAILS = 10;
  public static final String THRESHOLD_TO_LOG_REGION_DETAILS =
      "hbase.client.threshold.log.region.details";
  private static final int DEFAULT_THRESHOLD_TO_LOG_REGION_DETAILS = 2;
  private final int thresholdToLogRegionDetails;
  SimpleRequestController(final Configuration conf) {
    this.maxTotalConcurrentTasks = conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
            HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS);
    this.maxConcurrentTasksPerServer = conf.getInt(HConstants.HBASE_CLIENT_MAX_PERSERVER_TASKS,
            HConstants.DEFAULT_HBASE_CLIENT_MAX_PERSERVER_TASKS);
    this.maxConcurrentTasksPerRegion = conf.getInt(HConstants.HBASE_CLIENT_MAX_PERREGION_TASKS,
            HConstants.DEFAULT_HBASE_CLIENT_MAX_PERREGION_TASKS);
    this.maxHeapSizePerRequest = conf.getLong(HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE,
            DEFAULT_HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE);
    this.maxHeapSizeSubmit = conf.getLong(HBASE_CLIENT_MAX_SUBMIT_HEAPSIZE, DEFAULT_HBASE_CLIENT_MAX_SUBMIT_HEAPSIZE);
    this.thresholdToLogUndoneTaskDetails =
        conf.getInt(THRESHOLD_TO_LOG_UNDONE_TASK_DETAILS,
          DEFAULT_THRESHOLD_TO_LOG_UNDONE_TASK_DETAILS);
    this.thresholdToLogRegionDetails =
        conf.getInt(THRESHOLD_TO_LOG_REGION_DETAILS,
          DEFAULT_THRESHOLD_TO_LOG_REGION_DETAILS);
    if (this.maxTotalConcurrentTasks <= 0) {
      throw new IllegalArgumentException("maxTotalConcurrentTasks=" + maxTotalConcurrentTasks);
    }
    if (this.maxConcurrentTasksPerServer <= 0) {
      throw new IllegalArgumentException("maxConcurrentTasksPerServer="
              + maxConcurrentTasksPerServer);
    }
    if (this.maxConcurrentTasksPerRegion <= 0) {
      throw new IllegalArgumentException("maxConcurrentTasksPerRegion="
              + maxConcurrentTasksPerRegion);
    }
    if (this.maxHeapSizePerRequest <= 0) {
      throw new IllegalArgumentException("maxHeapSizePerServer="
              + maxHeapSizePerRequest);
    }

    if (this.maxHeapSizeSubmit <= 0) {
      throw new IllegalArgumentException("maxHeapSizeSubmit="
              + maxHeapSizeSubmit);
    }
  }

  @VisibleForTesting
  static Checker newChecker(List<RowChecker> checkers) {
    return new Checker() {
      private boolean isEnd = false;

      @Override
      public ReturnCode canTakeRow(HRegionLocation loc, Row row) {
        if (isEnd) {
          return ReturnCode.END;
        }
        long rowSize = (row instanceof Mutation) ? ((Mutation) row).heapSize() : 0;
        ReturnCode code = ReturnCode.INCLUDE;
        for (RowChecker checker : checkers) {
          switch (checker.canTakeOperation(loc, rowSize)) {
            case END:
              isEnd = true;
              code = ReturnCode.END;
              break;
            case SKIP:
              code = ReturnCode.SKIP;
              break;
            case INCLUDE:
            default:
              break;
          }
          if (code == ReturnCode.END) {
            break;
          }
        }
        for (RowChecker checker : checkers) {
          checker.notifyFinal(code, loc, rowSize);
        }
        return code;
      }

      @Override
      public void reset() throws InterruptedIOException {
        isEnd = false;
        InterruptedIOException e = null;
        for (RowChecker checker : checkers) {
          try {
            checker.reset();
          } catch (InterruptedIOException ex) {
            e = ex;
          }
        }
        if (e != null) {
          throw e;
        }
      }
    };
  }

  @Override
  public Checker newChecker() {
    List<RowChecker> checkers = new ArrayList<>(3);
    checkers.add(new TaskCountChecker(maxTotalConcurrentTasks,
            maxConcurrentTasksPerServer,
            maxConcurrentTasksPerRegion,
            tasksInProgress,
            taskCounterPerServer,
            taskCounterPerRegion));
    checkers.add(new RequestSizeChecker(maxHeapSizePerRequest));
    checkers.add(new SubmittedSizeChecker(maxHeapSizeSubmit));
    return newChecker(checkers);
  }

  @Override
  public void incTaskCounters(Collection<byte[]> regions, ServerName sn) {
    tasksInProgress.incrementAndGet();

    computeIfAbsent(taskCounterPerServer, sn, AtomicInteger::new).incrementAndGet();

    regions.forEach((regBytes)
            -> computeIfAbsent(taskCounterPerRegion, regBytes, AtomicInteger::new).incrementAndGet()
    );
  }

  @Override
  public void decTaskCounters(Collection<byte[]> regions, ServerName sn) {
    regions.forEach(regBytes -> {
      AtomicInteger regionCnt = taskCounterPerRegion.get(regBytes);
      regionCnt.decrementAndGet();
    });

    taskCounterPerServer.get(sn).decrementAndGet();
    tasksInProgress.decrementAndGet();
    synchronized (tasksInProgress) {
      tasksInProgress.notifyAll();
    }
  }

  @Override
  public long getNumberOfTasksInProgress() {
    return tasksInProgress.get();
  }

  @Override
  public void waitForMaximumCurrentTasks(long max, long id,
    int periodToTrigger, Consumer<Long> trigger) throws InterruptedIOException {
    assert max >= 0;
    long lastLog = EnvironmentEdgeManager.currentTime();
    long currentInProgress, oldInProgress = Long.MAX_VALUE;
    while ((currentInProgress = tasksInProgress.get()) > max) {
      if (oldInProgress != currentInProgress) { // Wait for in progress to change.
        long now = EnvironmentEdgeManager.currentTime();
        if (now > lastLog + periodToTrigger) {
          lastLog = now;
          if (trigger != null) {
            trigger.accept(currentInProgress);
          }
          logDetailsOfUndoneTasks(currentInProgress);
        }
      }
      oldInProgress = currentInProgress;
      try {
        synchronized (tasksInProgress) {
          if (tasksInProgress.get() == oldInProgress) {
            tasksInProgress.wait(10);
          }
        }
      } catch (InterruptedException e) {
        throw new InterruptedIOException("#" + id + ", interrupted." +
            " currentNumberOfTask=" + currentInProgress);
      }
    }
  }

  private void logDetailsOfUndoneTasks(long taskInProgress) {
    if (taskInProgress <= thresholdToLogUndoneTaskDetails) {
      ArrayList<ServerName> servers = new ArrayList<>();
      for (Map.Entry<ServerName, AtomicInteger> entry : taskCounterPerServer.entrySet()) {
        if (entry.getValue().get() > 0) {
          servers.add(entry.getKey());
        }
      }
      LOG.info("Left over " + taskInProgress + " task(s) are processed on server(s): " + servers);
    }

    if (taskInProgress <= thresholdToLogRegionDetails) {
      ArrayList<String> regions = new ArrayList<>();
      for (Map.Entry<byte[], AtomicInteger> entry : taskCounterPerRegion.entrySet()) {
        if (entry.getValue().get() > 0) {
          regions.add(Bytes.toString(entry.getKey()));
        }
      }
      LOG.info("Regions against which left over task(s) are processed: " + regions);
    }
  }

  @Override
  public void waitForFreeSlot(long id, int periodToTrigger, Consumer<Long> trigger) throws InterruptedIOException {
    waitForMaximumCurrentTasks(maxTotalConcurrentTasks - 1, id, periodToTrigger, trigger);
  }

  /**
   * limit the heapsize of total submitted data. Reduce the limit of heapsize
   * for submitting quickly if there is no running task.
   */
  @VisibleForTesting
  static class SubmittedSizeChecker implements RowChecker {

    private final long maxHeapSizeSubmit;
    private long heapSize = 0;

    SubmittedSizeChecker(final long maxHeapSizeSubmit) {
      this.maxHeapSizeSubmit = maxHeapSizeSubmit;
    }

    @Override
    public ReturnCode canTakeOperation(HRegionLocation loc, long rowSize) {
      if (heapSize >= maxHeapSizeSubmit) {
        return ReturnCode.END;
      }
      return ReturnCode.INCLUDE;
    }

    @Override
    public void notifyFinal(ReturnCode code, HRegionLocation loc, long rowSize) {
      if (code == ReturnCode.INCLUDE) {
        heapSize += rowSize;
      }
    }

    @Override
    public void reset() {
      heapSize = 0;
    }
  }

  /**
   * limit the max number of tasks in an AsyncProcess.
   */
  @VisibleForTesting
  static class TaskCountChecker implements RowChecker {

    private static final long MAX_WAITING_TIME = 1000; //ms
    private final Set<HRegionInfo> regionsIncluded = new HashSet<>();
    private final Set<ServerName> serversIncluded = new HashSet<>();
    private final int maxConcurrentTasksPerRegion;
    private final int maxTotalConcurrentTasks;
    private final int maxConcurrentTasksPerServer;
    private final Map<byte[], AtomicInteger> taskCounterPerRegion;
    private final Map<ServerName, AtomicInteger> taskCounterPerServer;
    private final Set<byte[]> busyRegions = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    private final AtomicLong tasksInProgress;

    TaskCountChecker(final int maxTotalConcurrentTasks,
            final int maxConcurrentTasksPerServer,
            final int maxConcurrentTasksPerRegion,
            final AtomicLong tasksInProgress,
            final Map<ServerName, AtomicInteger> taskCounterPerServer,
            final Map<byte[], AtomicInteger> taskCounterPerRegion) {
      this.maxTotalConcurrentTasks = maxTotalConcurrentTasks;
      this.maxConcurrentTasksPerRegion = maxConcurrentTasksPerRegion;
      this.maxConcurrentTasksPerServer = maxConcurrentTasksPerServer;
      this.taskCounterPerRegion = taskCounterPerRegion;
      this.taskCounterPerServer = taskCounterPerServer;
      this.tasksInProgress = tasksInProgress;
    }

    @Override
    public void reset() throws InterruptedIOException {
      // prevent the busy-waiting
      waitForRegion();
      regionsIncluded.clear();
      serversIncluded.clear();
      busyRegions.clear();
    }

    private void waitForRegion() throws InterruptedIOException {
      if (busyRegions.isEmpty()) {
        return;
      }
      EnvironmentEdge ee = EnvironmentEdgeManager.getDelegate();
      final long start = ee.currentTime();
      while ((ee.currentTime() - start) <= MAX_WAITING_TIME) {
        for (byte[] region : busyRegions) {
          AtomicInteger count = taskCounterPerRegion.get(region);
          if (count == null || count.get() < maxConcurrentTasksPerRegion) {
            return;
          }
        }
        try {
          synchronized (tasksInProgress) {
            tasksInProgress.wait(10);
          }
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted."
                  + " tasksInProgress=" + tasksInProgress);
        }
      }
    }

    /**
     * 1) check the regions is allowed. 2) check the concurrent tasks for
     * regions. 3) check the total concurrent tasks. 4) check the concurrent
     * tasks for server.
     *
     * @param loc
     * @param rowSize
     * @return
     */
    @Override
    public ReturnCode canTakeOperation(HRegionLocation loc, long rowSize) {

      HRegionInfo regionInfo = loc.getRegionInfo();
      if (regionsIncluded.contains(regionInfo)) {
        // We already know what to do with this region.
        return ReturnCode.INCLUDE;
      }
      AtomicInteger regionCnt = taskCounterPerRegion.get(loc.getRegionInfo().getRegionName());
      if (regionCnt != null && regionCnt.get() >= maxConcurrentTasksPerRegion) {
        // Too many tasks on this region already.
        return ReturnCode.SKIP;
      }
      int newServers = serversIncluded.size()
              + (serversIncluded.contains(loc.getServerName()) ? 0 : 1);
      if ((newServers + tasksInProgress.get()) > maxTotalConcurrentTasks) {
        // Too many tasks.
        return ReturnCode.SKIP;
      }
      AtomicInteger serverCnt = taskCounterPerServer.get(loc.getServerName());
      if (serverCnt != null && serverCnt.get() >= maxConcurrentTasksPerServer) {
        // Too many tasks for this individual server
        return ReturnCode.SKIP;
      }
      return ReturnCode.INCLUDE;
    }

    @Override
    public void notifyFinal(ReturnCode code, HRegionLocation loc, long rowSize) {
      if (code == ReturnCode.INCLUDE) {
        regionsIncluded.add(loc.getRegionInfo());
        serversIncluded.add(loc.getServerName());
      }
      busyRegions.add(loc.getRegionInfo().getRegionName());
    }
  }

  /**
   * limit the request size for each regionserver.
   */
  @VisibleForTesting
  static class RequestSizeChecker implements RowChecker {

    private final long maxHeapSizePerRequest;
    private final Map<ServerName, Long> serverRequestSizes = new HashMap<>();

    RequestSizeChecker(final long maxHeapSizePerRequest) {
      this.maxHeapSizePerRequest = maxHeapSizePerRequest;
    }

    @Override
    public void reset() {
      serverRequestSizes.clear();
    }

    @Override
    public ReturnCode canTakeOperation(HRegionLocation loc, long rowSize) {
      // Is it ok for limit of request size?
      long currentRequestSize = serverRequestSizes.containsKey(loc.getServerName())
              ? serverRequestSizes.get(loc.getServerName()) : 0L;
      // accept at least one request
      if (currentRequestSize == 0 || currentRequestSize + rowSize <= maxHeapSizePerRequest) {
        return ReturnCode.INCLUDE;
      }
      return ReturnCode.SKIP;
    }

    @Override
    public void notifyFinal(ReturnCode code, HRegionLocation loc, long rowSize) {
      if (code == ReturnCode.INCLUDE) {
        long currentRequestSize = serverRequestSizes.containsKey(loc.getServerName())
                ? serverRequestSizes.get(loc.getServerName()) : 0L;
        serverRequestSizes.put(loc.getServerName(), currentRequestSize + rowSize);
      }
    }
  }

  /**
   * Provide a way to control the flow of rows iteration.
   */
  @VisibleForTesting
  interface RowChecker {

    ReturnCode canTakeOperation(HRegionLocation loc, long rowSize);

    /**
     * Add the final ReturnCode to the checker. The ReturnCode may be reversed,
     * so the checker need the final decision to update the inner state.
     *
     * @param code The final decision
     * @param loc the destination of data
     * @param rowSize the data size
     */
    void notifyFinal(ReturnCode code, HRegionLocation loc, long rowSize);

    /**
     * Reset the inner state.
     */
    void reset() throws InterruptedIOException;
  }
}
