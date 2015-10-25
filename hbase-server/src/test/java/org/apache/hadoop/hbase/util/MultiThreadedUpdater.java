/**
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

package org.apache.hadoop.hbase.util;

import static org.apache.hadoop.hbase.util.test.LoadTestDataGenerator.INCREMENT;
import static org.apache.hadoop.hbase.util.test.LoadTestDataGenerator.MUTATE_INFO;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.OperationConflictException;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.util.test.LoadTestDataGenerator;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;

/** Creates multiple threads that write key/values into the */
public class MultiThreadedUpdater extends MultiThreadedWriterBase {
  private static final Log LOG = LogFactory.getLog(MultiThreadedUpdater.class);

  protected Set<HBaseUpdaterThread> updaters = new HashSet<HBaseUpdaterThread>();

  private MultiThreadedWriterBase writer = null;
  private boolean isBatchUpdate = false;
  private boolean ignoreNonceConflicts = false;
  private final double updatePercent;

  public MultiThreadedUpdater(LoadTestDataGenerator dataGen, Configuration conf,
      TableName tableName, double updatePercent) throws IOException {
    super(dataGen, conf, tableName, "U");
    this.updatePercent = updatePercent;
  }

  /** Use batch vs. separate updates for every column in a row */
  public void setBatchUpdate(boolean isBatchUpdate) {
    this.isBatchUpdate = isBatchUpdate;
  }

  public void linkToWriter(MultiThreadedWriterBase writer) {
    this.writer = writer;
    writer.setTrackWroteKeys(true);
  }

  @Override
  public void start(long startKey, long endKey, int numThreads) throws IOException {
    super.start(startKey, endKey, numThreads);

    if (verbose) {
      LOG.debug("Updating keys [" + startKey + ", " + endKey + ")");
    }

    addUpdaterThreads(numThreads);

    startThreads(updaters);
  }

  protected void addUpdaterThreads(int numThreads) throws IOException {
    for (int i = 0; i < numThreads; ++i) {
      HBaseUpdaterThread updater = new HBaseUpdaterThread(i);
      updaters.add(updater);
    }
  }

  private long getNextKeyToUpdate() {
    if (writer == null) {
      return nextKeyToWrite.getAndIncrement();
    }
    synchronized (this) {
      if (nextKeyToWrite.get() >= endKey) {
        // Finished the whole key range
        return endKey;
      }
      while (nextKeyToWrite.get() > writer.wroteUpToKey()) {
        Threads.sleepWithoutInterrupt(100);
      }
      long k = nextKeyToWrite.getAndIncrement();
      if (writer.failedToWriteKey(k)) {
        failedKeySet.add(k);
        return getNextKeyToUpdate();
      }
      return k;
    }
  }

  protected class HBaseUpdaterThread extends Thread {
    protected final Table table;

    public HBaseUpdaterThread(int updaterId) throws IOException {
      setName(getClass().getSimpleName() + "_" + updaterId);
      table = createTable();
    }

    protected HTableInterface createTable() throws IOException {
      return connection.getTable(tableName);
    }

    @Override
    public void run() {
      try {
        long rowKeyBase;
        StringBuilder buf = new StringBuilder();
        byte[][] columnFamilies = dataGenerator.getColumnFamilies();
        while ((rowKeyBase = getNextKeyToUpdate()) < endKey) {
          if (RandomUtils.nextInt(100) < updatePercent) {
            byte[] rowKey = dataGenerator.getDeterministicUniqueKey(rowKeyBase);
            Increment inc = new Increment(rowKey);
            Append app = new Append(rowKey);
            numKeys.addAndGet(1);
            int columnCount = 0;
            for (byte[] cf : columnFamilies) {
              long cfHash = Arrays.hashCode(cf);
              inc.addColumn(cf, INCREMENT, cfHash);
              buf.setLength(0); // Clear the buffer
              buf.append("#").append(Bytes.toString(INCREMENT));
              buf.append(":").append(MutationType.INCREMENT.getNumber());
              app.add(cf, MUTATE_INFO, Bytes.toBytes(buf.toString()));
              ++columnCount;
              if (!isBatchUpdate) {
                mutate(table, inc, rowKeyBase);
                numCols.addAndGet(1);
                inc = new Increment(rowKey);
                mutate(table, app, rowKeyBase);
                numCols.addAndGet(1);
                app = new Append(rowKey);
              }
              Get get = new Get(rowKey);
              get.addFamily(cf);
              try {
                get = dataGenerator.beforeGet(rowKeyBase, get);
              } catch (Exception e) {
                // Ideally wont happen
                LOG.warn("Failed to modify the get from the load generator  = [" + get.getRow()
                    + "], column family = [" + Bytes.toString(cf) + "]", e);
              }
              Result result = getRow(get, rowKeyBase, cf);
              Map<byte[], byte[]> columnValues =
                result != null ? result.getFamilyMap(cf) : null;
              if (columnValues == null) {
                int specialPermCellInsertionFactor = Integer.parseInt(dataGenerator.getArgs()[2]);
                if (((int) rowKeyBase % specialPermCellInsertionFactor == 0)) {
                  LOG.info("Null result expected for the rowkey " + Bytes.toString(rowKey));
                } else {
                  failedKeySet.add(rowKeyBase);
                  LOG.error("Failed to update the row with key = [" + rowKey
                      + "], since we could not get the original row");
                }
              }
              if(columnValues != null) {
                for (byte[] column : columnValues.keySet()) {
                  if (Bytes.equals(column, INCREMENT) || Bytes.equals(column, MUTATE_INFO)) {
                    continue;
                  }
                  MutationType mt = MutationType
                      .valueOf(RandomUtils.nextInt(MutationType.values().length));
                  long columnHash = Arrays.hashCode(column);
                  long hashCode = cfHash + columnHash;
                  byte[] hashCodeBytes = Bytes.toBytes(hashCode);
                  byte[] checkedValue = HConstants.EMPTY_BYTE_ARRAY;
                  if (hashCode % 2 == 0) {
                    Cell kv = result.getColumnLatestCell(cf, column);
                    checkedValue = kv != null ? CellUtil.cloneValue(kv) : null;
                    Preconditions.checkNotNull(checkedValue,
                        "Column value to be checked should not be null");
                  }
                  buf.setLength(0); // Clear the buffer
                  buf.append("#").append(Bytes.toString(column)).append(":");
                  ++columnCount;
                  switch (mt) {
                  case PUT:
                    Put put = new Put(rowKey);
                    put.addColumn(cf, column, hashCodeBytes);
                    mutate(table, put, rowKeyBase, rowKey, cf, column, checkedValue);
                    buf.append(MutationType.PUT.getNumber());
                    break;
                  case DELETE:
                    Delete delete = new Delete(rowKey);
                    // Delete all versions since a put
                    // could be called multiple times if CM is used
                    delete.deleteColumns(cf, column);
                    mutate(table, delete, rowKeyBase, rowKey, cf, column, checkedValue);
                    buf.append(MutationType.DELETE.getNumber());
                    break;
                  default:
                    buf.append(MutationType.APPEND.getNumber());
                    app.add(cf, column, hashCodeBytes);
                  }
                  app.add(cf, MUTATE_INFO, Bytes.toBytes(buf.toString()));
                  if (!isBatchUpdate) {
                    mutate(table, app, rowKeyBase);
                    numCols.addAndGet(1);
                    app = new Append(rowKey);
                  }
                }
              }
            }
            if (isBatchUpdate) {
              if (verbose) {
                LOG.debug("Preparing increment and append for key = ["
                  + rowKey + "], " + columnCount + " columns");
              }
              mutate(table, inc, rowKeyBase);
              mutate(table, app, rowKeyBase);
              numCols.addAndGet(columnCount);
            }
          }
          if (trackWroteKeys) {
            wroteKeys.add(rowKeyBase);
          }
        }
      } finally {
        closeHTable();
        numThreadsWorking.decrementAndGet();
      }
    }

    protected void closeHTable() {
      try {
        if (table != null) {
          table.close();
        }
      } catch (IOException e) {
        LOG.error("Error closing table", e);
      }
    }

    protected Result getRow(Get get, long rowKeyBase, byte[] cf) {
      Result result = null;
      try {
        result = table.get(get);
      } catch (IOException ie) {
        LOG.warn(
            "Failed to get the row for key = [" + get.getRow() + "], column family = ["
                + Bytes.toString(cf) + "]", ie);
      }
      return result;
    }

    public void mutate(Table table, Mutation m, long keyBase) {
      mutate(table, m, keyBase, null, null, null, null);
    }

    public void mutate(Table table, Mutation m,
        long keyBase, byte[] row, byte[] cf, byte[] q, byte[] v) {
      long start = System.currentTimeMillis();
      try {
        m = dataGenerator.beforeMutate(keyBase, m);
        if (m instanceof Increment) {
          table.increment((Increment)m);
        } else if (m instanceof Append) {
          table.append((Append)m);
        } else if (m instanceof Put) {
          table.checkAndPut(row, cf, q, v, (Put)m);
        } else if (m instanceof Delete) {
          table.checkAndDelete(row, cf, q, v, (Delete)m);
        } else {
          throw new IllegalArgumentException(
            "unsupported mutation " + m.getClass().getSimpleName());
        }
        totalOpTimeMs.addAndGet(System.currentTimeMillis() - start);
      } catch (IOException e) {
        if (ignoreNonceConflicts && (e instanceof OperationConflictException)) {
          LOG.info("Detected nonce conflict, ignoring: " + e.getMessage());
          totalOpTimeMs.addAndGet(System.currentTimeMillis() - start);
          return;
        }
        failedKeySet.add(keyBase);
        String exceptionInfo;
        if (e instanceof RetriesExhaustedWithDetailsException) {
          RetriesExhaustedWithDetailsException aggEx = (RetriesExhaustedWithDetailsException) e;
          exceptionInfo = aggEx.getExhaustiveDescription();
        } else {
          exceptionInfo = StringUtils.stringifyException(e);
        }
        LOG.error("Failed to mutate: " + keyBase + " after " +
            (System.currentTimeMillis() - start) +
          "ms; region information: " + getRegionDebugInfoSafe(table, m.getRow()) + "; errors: "
            + exceptionInfo);
      }
    }
  }

  @Override
  public void waitForFinish() {
    super.waitForFinish();
    System.out.println("Failed to update keys: " + failedKeySet.size());
    for (Long key : failedKeySet) {
       System.out.println("Failed to update key: " + key);
    }
  }

  public void mutate(Table table, Mutation m, long keyBase) {
    mutate(table, m, keyBase, null, null, null, null);
  }

  public void mutate(Table table, Mutation m,
      long keyBase, byte[] row, byte[] cf, byte[] q, byte[] v) {
    long start = System.currentTimeMillis();
    try {
      m = dataGenerator.beforeMutate(keyBase, m);
      if (m instanceof Increment) {
        table.increment((Increment)m);
      } else if (m instanceof Append) {
        table.append((Append)m);
      } else if (m instanceof Put) {
        table.checkAndPut(row, cf, q, v, (Put)m);
      } else if (m instanceof Delete) {
        table.checkAndDelete(row, cf, q, v, (Delete)m);
      } else {
        throw new IllegalArgumentException(
          "unsupported mutation " + m.getClass().getSimpleName());
      }
      totalOpTimeMs.addAndGet(System.currentTimeMillis() - start);
    } catch (IOException e) {
      failedKeySet.add(keyBase);
      String exceptionInfo;
      if (e instanceof RetriesExhaustedWithDetailsException) {
        RetriesExhaustedWithDetailsException aggEx = (RetriesExhaustedWithDetailsException)e;
        exceptionInfo = aggEx.getExhaustiveDescription();
      } else {
        StringWriter stackWriter = new StringWriter();
        PrintWriter pw = new PrintWriter(stackWriter);
        e.printStackTrace(pw);
        pw.flush();
        exceptionInfo = StringUtils.stringifyException(e);
      }
      LOG.error("Failed to mutate: " + keyBase + " after " + (System.currentTimeMillis() - start) +
        "ms; region information: " + getRegionDebugInfoSafe(table, m.getRow()) + "; errors: "
          + exceptionInfo);
    }
  }

  public void setIgnoreNonceConflicts(boolean value) {
    this.ignoreNonceConflicts = value;
  }
}
