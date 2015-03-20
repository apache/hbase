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

import static org.apache.hadoop.hbase.util.test.LoadTestDataGenerator.INCREMENT;
import static org.apache.hadoop.hbase.util.test.LoadTestDataGenerator.MUTATE_INFO;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.util.test.LoadTestDataGenerator;
import org.apache.hadoop.hbase.util.test.LoadTestKVGenerator;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;

/**
 * Common base class for reader and writer parts of multi-thread HBase load
 * test ({@link LoadTestTool}).
 */
public abstract class MultiThreadedAction {
  private static final Log LOG = LogFactory.getLog(MultiThreadedAction.class);

  protected final TableName tableName;
  protected final Configuration conf;
  protected final HConnection connection; // all reader / writer threads will share this connection

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

  protected LoadTestDataGenerator dataGenerator = null;

  /**
   * Default implementation of LoadTestDataGenerator that uses LoadTestKVGenerator, fixed
   * set of column families, and random number of columns in range. The table for it can
   * be created manually or, for example, via
   * {@link HBaseTestingUtility#createPreSplitLoadTestTable(
   * org.apache.hadoop.hbase.Configuration, byte[], byte[], Algorithm, DataBlockEncoding)}
   */
  public static class DefaultDataGenerator extends LoadTestDataGenerator {
    private byte[][] columnFamilies = null;
    private int minColumnsPerKey;
    private int maxColumnsPerKey;
    private final Random random = new Random();

    public DefaultDataGenerator(int minValueSize, int maxValueSize,
        int minColumnsPerKey, int maxColumnsPerKey, byte[]... columnFamilies) {
      super(minValueSize, maxValueSize);
      this.columnFamilies = columnFamilies;
      this.minColumnsPerKey = minColumnsPerKey;
      this.maxColumnsPerKey = maxColumnsPerKey;
    }

    public DefaultDataGenerator(byte[]... columnFamilies) {
      // Default values for tests that didn't care to provide theirs.
      this(256, 1024, 1, 10, columnFamilies);
    }

    @Override
    public byte[] getDeterministicUniqueKey(long keyBase) {
      return LoadTestKVGenerator.md5PrefixedKey(keyBase).getBytes();
    }

    @Override
    public byte[][] getColumnFamilies() {
      return columnFamilies;
    }

    @Override
    public byte[][] generateColumnsForCf(byte[] rowKey, byte[] cf) {
      int numColumns = minColumnsPerKey + random.nextInt(maxColumnsPerKey - minColumnsPerKey + 1);
      byte[][] columns = new byte[numColumns][];
      for (int i = 0; i < numColumns; ++i) {
        columns[i] = Integer.toString(i).getBytes();
      }
      return columns;
    }

    @Override
    public byte[] generateValue(byte[] rowKey, byte[] cf, byte[] column) {
      return kvGenerator.generateRandomSizeValue(rowKey, cf, column);
    }

    @Override
    public boolean verify(byte[] rowKey, byte[] cf, byte[] column, byte[] value) {
      return LoadTestKVGenerator.verify(value, rowKey, cf, column);
    }

    @Override
    public boolean verify(byte[] rowKey, byte[] cf, Set<byte[]> columnSet) {
      return (columnSet.size() >= minColumnsPerKey) && (columnSet.size() <= maxColumnsPerKey);
    }
  }

  /** "R" or "W" */
  private String actionLetter;

  /** Whether we need to print out Hadoop Streaming-style counters */
  private boolean streamingCounters;

  public static final int REPORTING_INTERVAL_MS = 5000;

  public MultiThreadedAction(LoadTestDataGenerator dataGen, Configuration conf,
                             TableName tableName,
                             String actionLetter) throws IOException {
    this.conf = conf;
    this.dataGenerator = dataGen;
    this.tableName = tableName;
    this.actionLetter = actionLetter;
    this.connection = (HConnection) ConnectionFactory.createConnection(conf);
  }

  public void start(long startKey, long endKey, int numThreads) throws IOException {
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

  public void close() {
    if (connection != null) {
      try {
        connection.close();
      } catch (Exception ex) {
        LOG.warn("Could not close the connection: " + ex);
      }
    }
  }

  public void waitForFinish() {
    while (numThreadsWorking.get() != 0) {
      Threads.sleepWithoutInterrupt(1000);
    }
    close();
  }

  public boolean isDone() {
    return (numThreadsWorking.get() == 0);
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

  protected static void appendToStatus(StringBuilder sb, String desc,
      String v) {
    sb.append(", ");
    sb.append(desc);
    sb.append("=");
    sb.append(v);
  }

  /**
   * See {@link #verifyResultAgainstDataGenerator(Result, boolean, boolean)}.
   * Does not verify cf/column integrity.
   */
  public boolean verifyResultAgainstDataGenerator(Result result, boolean verifyValues) {
    return verifyResultAgainstDataGenerator(result, verifyValues, false);
  }

  /**
   * Verifies the result from get or scan using the dataGenerator (that was presumably
   * also used to generate said result).
   * @param verifyValues verify that values in the result make sense for row/cf/column combination
   * @param verifyCfAndColumnIntegrity verify that cf/column set in the result is complete. Note
   *                                   that to use this multiPut should be used, or verification
   *                                   has to happen after writes, otherwise there can be races.
   * @return
   */
  public boolean verifyResultAgainstDataGenerator(Result result, boolean verifyValues,
      boolean verifyCfAndColumnIntegrity) {
    String rowKeyStr = Bytes.toString(result.getRow());
    // See if we have any data at all.
    if (result.isEmpty()) {
      LOG.error("Error checking data for key [" + rowKeyStr + "], no data returned");
      printLocations(result);
      return false;
    }

    if (!verifyValues && !verifyCfAndColumnIntegrity) {
      return true; // as long as we have something, we are good.
    }

    // See if we have all the CFs.
    byte[][] expectedCfs = dataGenerator.getColumnFamilies();
    if (verifyCfAndColumnIntegrity && (expectedCfs.length != result.getMap().size())) {
      LOG.error("Error checking data for key [" + rowKeyStr
        + "], bad family count: " + result.getMap().size());
      printLocations(result);
      return false;
    }

    // Verify each column family from get in the result.
    for (byte[] cf : result.getMap().keySet()) {
      String cfStr = Bytes.toString(cf);
      Map<byte[], byte[]> columnValues = result.getFamilyMap(cf);
      if (columnValues == null) {
        LOG.error("Error checking data for key [" + rowKeyStr
          + "], no data for family [" + cfStr + "]]");
        printLocations(result);
        return false;
      }

      Map<String, MutationType> mutateInfo = null;
      if (verifyCfAndColumnIntegrity || verifyValues) {
        if (!columnValues.containsKey(MUTATE_INFO)) {
          LOG.error("Error checking data for key [" + rowKeyStr + "], column family ["
            + cfStr + "], column [" + Bytes.toString(MUTATE_INFO) + "]; value is not found");
          printLocations(result);
          return false;
        }

        long cfHash = Arrays.hashCode(cf);
        // Verify deleted columns, and make up column counts if deleted
        byte[] mutateInfoValue = columnValues.remove(MUTATE_INFO);
        mutateInfo = parseMutateInfo(mutateInfoValue);
        for (Map.Entry<String, MutationType> mutate: mutateInfo.entrySet()) {
          if (mutate.getValue() == MutationType.DELETE) {
            byte[] column = Bytes.toBytes(mutate.getKey());
            long columnHash = Arrays.hashCode(column);
            long hashCode = cfHash + columnHash;
            if (hashCode % 2 == 0) {
              if (columnValues.containsKey(column)) {
                LOG.error("Error checking data for key [" + rowKeyStr + "], column family ["
                  + cfStr + "], column [" + mutate.getKey() + "]; should be deleted");
                printLocations(result);
                return false;
              }
              byte[] hashCodeBytes = Bytes.toBytes(hashCode);
              columnValues.put(column, hashCodeBytes);
            }
          }
        }

        // Verify increment
        if (!columnValues.containsKey(INCREMENT)) {
          LOG.error("Error checking data for key [" + rowKeyStr + "], column family ["
            + cfStr + "], column [" + Bytes.toString(INCREMENT) + "]; value is not found");
          printLocations(result);
          return false;
        }
        long currentValue = Bytes.toLong(columnValues.remove(INCREMENT));
        if (verifyValues) {
          long amount = mutateInfo.isEmpty() ? 0 : cfHash;
          long originalValue = Arrays.hashCode(result.getRow());
          long extra = currentValue - originalValue;
          if (extra != 0 && (amount == 0 || extra % amount != 0)) {
            LOG.error("Error checking data for key [" + rowKeyStr + "], column family ["
              + cfStr + "], column [increment], extra [" + extra + "], amount [" + amount + "]");
            printLocations(result);
            return false;
          }
          if (amount != 0 && extra != amount) {
            LOG.warn("Warning checking data for key [" + rowKeyStr + "], column family ["
              + cfStr + "], column [increment], incremented [" + (extra / amount) + "] times");
          }
        }

        // See if we have correct columns.
        if (verifyCfAndColumnIntegrity
            && !dataGenerator.verify(result.getRow(), cf, columnValues.keySet())) {
          String colsStr = "";
          for (byte[] col : columnValues.keySet()) {
            if (colsStr.length() > 0) {
              colsStr += ", ";
            }
            colsStr += "[" + Bytes.toString(col) + "]";
          }
          LOG.error("Error checking data for key [" + rowKeyStr
            + "], bad columns for family [" + cfStr + "]: " + colsStr);
          printLocations(result);
          return false;
        }
        // See if values check out.
        if (verifyValues) {
          for (Map.Entry<byte[], byte[]> kv : columnValues.entrySet()) {
            String column = Bytes.toString(kv.getKey());
            MutationType mutation = mutateInfo.get(column);
            boolean verificationNeeded = true;
            byte[] bytes = kv.getValue();
            if (mutation != null) {
              boolean mutationVerified = true;
              long columnHash = Arrays.hashCode(kv.getKey());
              long hashCode = cfHash + columnHash;
              byte[] hashCodeBytes = Bytes.toBytes(hashCode);
              if (mutation == MutationType.APPEND) {
                int offset = bytes.length - hashCodeBytes.length;
                mutationVerified = offset > 0 && Bytes.equals(hashCodeBytes,
                  0, hashCodeBytes.length, bytes, offset, hashCodeBytes.length);
                if (mutationVerified) {
                  int n = 1;
                  while (true) {
                    int newOffset = offset - hashCodeBytes.length;
                    if (newOffset < 0 || !Bytes.equals(hashCodeBytes, 0,
                        hashCodeBytes.length, bytes, newOffset, hashCodeBytes.length)) {
                      break;
                    }
                    offset = newOffset;
                    n++;
                  }
                  if (n > 1) {
                    LOG.warn("Warning checking data for key [" + rowKeyStr + "], column family ["
                      + cfStr + "], column [" + column + "], appended [" + n + "] times");
                  }
                  byte[] dest = new byte[offset];
                  System.arraycopy(bytes, 0, dest, 0, offset);
                  bytes = dest;
                }
              } else if (hashCode % 2 == 0) { // checkAndPut
                mutationVerified = Bytes.equals(bytes, hashCodeBytes);
                verificationNeeded = false;
              }
              if (!mutationVerified) {
                LOG.error("Error checking data for key [" + rowKeyStr
                  + "], mutation checking failed for column family [" + cfStr + "], column ["
                  + column + "]; mutation [" + mutation + "], hashCode ["
                  + hashCode + "], verificationNeeded ["
                  + verificationNeeded + "]");
                printLocations(result);
                return false;
              }
            } // end of mutation checking
            if (verificationNeeded &&
                !dataGenerator.verify(result.getRow(), cf, kv.getKey(), bytes)) {
              LOG.error("Error checking data for key [" + rowKeyStr + "], column family ["
                + cfStr + "], column [" + column + "], mutation [" + mutation
                + "]; value of length " + bytes.length);
              printLocations(result);
              return false;
            }
          }
        }
      }
    }
    return true;
  }

  private void printLocations(Result r) {
    RegionLocations rl = null;
    if (r == null) {
      LOG.info("FAILED FOR null Result");
      return;
    }
    LOG.info("FAILED FOR " + resultToString(r) + " Stale " + r.isStale());
    if (r.getRow() == null) {
      return;
    }
    try {
      rl = ((ClusterConnection)connection).locateRegion(tableName, r.getRow(), true, true);
    } catch (IOException e) {
      LOG.warn("Couldn't get locations for row " + Bytes.toString(r.getRow()));
    }
    HRegionLocation locations[] = rl.getRegionLocations();
    for (HRegionLocation h : locations) {
      LOG.info("LOCATION " + h);
    }
  }

  private String resultToString(Result result) {
    StringBuilder sb = new StringBuilder();
    sb.append("cells=");
    if(result.isEmpty()) {
      sb.append("NONE");
      return sb.toString();
    }
    sb.append("{");
    boolean moreThanOne = false;
    for(Cell cell : result.listCells()) {
      if(moreThanOne) {
        sb.append(", ");
      } else {
        moreThanOne = true;
      }
      sb.append(CellUtil.toString(cell, true));
    }
    sb.append("}");
    return sb.toString();
  }

  // Parse mutate info into a map of <column name> => <update action>
  private Map<String, MutationType> parseMutateInfo(byte[] mutateInfo) {
    Map<String, MutationType> mi = new HashMap<String, MutationType>();
    if (mutateInfo != null) {
      String mutateInfoStr = Bytes.toString(mutateInfo);
      String[] mutations = mutateInfoStr.split("#");
      for (String mutation: mutations) {
        if (mutation.isEmpty()) continue;
        Preconditions.checkArgument(mutation.contains(":"),
          "Invalid mutation info " + mutation);
        int p = mutation.indexOf(":");
        String column = mutation.substring(0, p);
        MutationType type = MutationType.valueOf(
          Integer.parseInt(mutation.substring(p+1)));
        mi.put(column, type);
      }
    }
    return mi;
  }
}
