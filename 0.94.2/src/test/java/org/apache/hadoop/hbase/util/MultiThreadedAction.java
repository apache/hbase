/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

/**
 * Common base class for reader and writer parts of multi-thread HBase load
 * test ({@link LoadTestTool}).
 */
public abstract class MultiThreadedAction {
  private static final Log LOG = LogFactory.getLog(MultiThreadedAction.class);

  protected final byte[] tableName;
  protected final byte[] columnFamily;
  protected final Configuration conf;

  protected int numThreads = 1;

  /** The start key of the key range, inclusive */
  protected long startKey = 0;

  /** The end key of the key range, exclusive */
  protected long endKey = 1;

  protected AtomicInteger numThreadsWorking = new AtomicInteger();
  protected AtomicLong numKeys = new AtomicLong();
  protected AtomicLong numCols = new AtomicLong();
  protected AtomicLong totalOpTimeMs = new AtomicLong();
  protected boolean verbose = false;

  protected int minDataSize = 256;
  protected int maxDataSize = 1024;

  /** "R" or "W" */
  private String actionLetter;

  /** Whether we need to print out Hadoop Streaming-style counters */
  private boolean streamingCounters;

  public static final int REPORTING_INTERVAL_MS = 5000;

  public MultiThreadedAction(Configuration conf, byte[] tableName,
      byte[] columnFamily, String actionLetter) {
    this.conf = conf;
    this.tableName = tableName;
    this.columnFamily = columnFamily;
    this.actionLetter = actionLetter;
  }

  public void start(long startKey, long endKey, int numThreads)
      throws IOException {
    this.startKey = startKey;
    this.endKey = endKey;
    this.numThreads = numThreads;
    (new Thread(new ProgressReporter(actionLetter))).start();
  }

  private static String formatTime(long elapsedTime) {
    String format = String.format("%%0%dd", 2);
    elapsedTime = elapsedTime / 1000;
    String seconds = String.format(format, elapsedTime % 60);
    String minutes = String.format(format, (elapsedTime % 3600) / 60);
    String hours = String.format(format, elapsedTime / 3600);
    String time =  hours + ":" + minutes + ":" + seconds;
    return time;
  }

  /** Asynchronously reports progress */
  private class ProgressReporter implements Runnable {

    private String reporterId = "";

    public ProgressReporter(String id) {
      this.reporterId = id;
    }

    @Override
    public void run() {
      long startTime = System.currentTimeMillis();
      long priorNumKeys = 0;
      long priorCumulativeOpTime = 0;
      int priorAverageKeysPerSecond = 0;

      // Give other threads time to start.
      Threads.sleep(REPORTING_INTERVAL_MS);

      while (numThreadsWorking.get() != 0) {
        String threadsLeft =
            "[" + reporterId + ":" + numThreadsWorking.get() + "] ";
        if (numKeys.get() == 0) {
          LOG.info(threadsLeft + "Number of keys = 0");
        } else {
          long numKeys = MultiThreadedAction.this.numKeys.get();
          long time = System.currentTimeMillis() - startTime;
          long totalOpTime = totalOpTimeMs.get();

          long numKeysDelta = numKeys - priorNumKeys;
          long totalOpTimeDelta = totalOpTime - priorCumulativeOpTime;

          double averageKeysPerSecond =
              (time > 0) ? (numKeys * 1000 / time) : 0;

          LOG.info(threadsLeft
              + "Keys="
              + numKeys
              + ", cols="
              + StringUtils.humanReadableInt(numCols.get())
              + ", time="
              + formatTime(time)
              + ((numKeys > 0 && time > 0) ? (" Overall: [" + "keys/s= "
                  + numKeys * 1000 / time + ", latency=" + totalOpTime
                  / numKeys + " ms]") : "")
              + ((numKeysDelta > 0) ? (" Current: [" + "keys/s="
                  + numKeysDelta * 1000 / REPORTING_INTERVAL_MS + ", latency="
                  + totalOpTimeDelta / numKeysDelta + " ms]") : "")
              + progressInfo());

          if (streamingCounters) {
            printStreamingCounters(numKeysDelta,
                averageKeysPerSecond - priorAverageKeysPerSecond);
          }

          priorNumKeys = numKeys;
          priorCumulativeOpTime = totalOpTime;
          priorAverageKeysPerSecond = (int) averageKeysPerSecond;
        }

        Threads.sleep(REPORTING_INTERVAL_MS);
      }
    }

    private void printStreamingCounters(long numKeysDelta,
        double avgKeysPerSecondDelta) {
      // Write stats in a format that can be interpreted as counters by
      // streaming map-reduce jobs.
      System.err.println("reporter:counter:numKeys," + reporterId + ","
          + numKeysDelta);
      System.err.println("reporter:counter:numCols," + reporterId + ","
          + numCols.get());
      System.err.println("reporter:counter:avgKeysPerSecond," + reporterId
          + "," + (long) (avgKeysPerSecondDelta));
    }
  }

  public void setDataSize(int minDataSize, int maxDataSize) {
    this.minDataSize = minDataSize;
    this.maxDataSize = maxDataSize;
  }

  public void waitForFinish() {
    while (numThreadsWorking.get() != 0) {
      Threads.sleepWithoutInterrupt(1000);
    }
  }

  protected void startThreads(Collection<? extends Thread> threads) {
    numThreadsWorking.addAndGet(threads.size());
    for (Thread thread : threads) {
      thread.start();
    }
  }

  /** @return the end key of the key range, exclusive */
  public long getEndKey() {
    return endKey;
  }

  /** Returns a task-specific progress string */
  protected abstract String progressInfo();

  protected static void appendToStatus(StringBuilder sb, String desc,
      long v) {
    if (v == 0) {
      return;
    }
    sb.append(", ");
    sb.append(desc);
    sb.append("=");
    sb.append(v);
  }

}
