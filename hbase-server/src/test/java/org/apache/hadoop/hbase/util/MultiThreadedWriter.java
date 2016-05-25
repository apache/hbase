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
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.test.LoadTestDataGenerator;
import org.apache.hadoop.util.StringUtils;

/** Creates multiple threads that write key/values into the */
public class MultiThreadedWriter extends MultiThreadedWriterBase {
  private static final Log LOG = LogFactory.getLog(MultiThreadedWriter.class);

  protected Set<HBaseWriterThread> writers = new HashSet<HBaseWriterThread>();

  protected boolean isMultiPut = false;

  public MultiThreadedWriter(LoadTestDataGenerator dataGen, Configuration conf,
      TableName tableName) throws IOException {
    super(dataGen, conf, tableName, "W");
  }

  /** Use multi-puts vs. separate puts for every column in a row */
  public void setMultiPut(boolean isMultiPut) {
    this.isMultiPut = isMultiPut;
  }

  @Override
  public void start(long startKey, long endKey, int numThreads) throws IOException {
    super.start(startKey, endKey, numThreads);

    if (verbose) {
      LOG.debug("Inserting keys [" + startKey + ", " + endKey + ")");
    }

    createWriterThreads(numThreads);

    startThreads(writers);
  }

  protected void createWriterThreads(int numThreads) throws IOException {
    for (int i = 0; i < numThreads; ++i) {
      HBaseWriterThread writer = new HBaseWriterThread(i);
      Threads.setLoggingUncaughtExceptionHandler(writer);
      writers.add(writer);
    }
  }

  public class HBaseWriterThread extends Thread {
    private final Table table;

    public HBaseWriterThread(int writerId) throws IOException {
      setName(getClass().getSimpleName() + "_" + writerId);
      table = createTable();
    }

    protected Table createTable() throws IOException {
      return connection.getTable(tableName);
    }

    @Override
    public void run() {
      try {
        long rowKeyBase;
        byte[][] columnFamilies = dataGenerator.getColumnFamilies();
        while ((rowKeyBase = nextKeyToWrite.getAndIncrement()) < endKey) {
          byte[] rowKey = dataGenerator.getDeterministicUniqueKey(rowKeyBase);
          Put put = new Put(rowKey);
          numKeys.addAndGet(1);
          int columnCount = 0;
          for (byte[] cf : columnFamilies) {
            byte[][] columns = dataGenerator.generateColumnsForCf(rowKey, cf);
            for (byte[] column : columns) {
              byte[] value = dataGenerator.generateValue(rowKey, cf, column);
              put.addColumn(cf, column, value);
              ++columnCount;
              if (!isMultiPut) {
                insert(table, put, rowKeyBase);
                numCols.addAndGet(1);
                put = new Put(rowKey);
              }
            }
            long rowKeyHash = Arrays.hashCode(rowKey);
            put.addColumn(cf, MUTATE_INFO, HConstants.EMPTY_BYTE_ARRAY);
            put.addColumn(cf, INCREMENT, Bytes.toBytes(rowKeyHash));
            if (!isMultiPut) {
              insert(table, put, rowKeyBase);
              numCols.addAndGet(1);
              put = new Put(rowKey);
            }
          }
          if (isMultiPut) {
            if (verbose) {
              LOG.debug("Preparing put for key = [" + rowKey + "], " + columnCount + " columns");
            }
            insert(table, put, rowKeyBase);
            numCols.addAndGet(columnCount);
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

    public void insert(Table table, Put put, long keyBase) {
      long start = System.currentTimeMillis();
      try {
        put = (Put) dataGenerator.beforeMutate(keyBase, put);
        table.put(put);
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
        LOG.error("Failed to insert: " + keyBase + " after " + (System.currentTimeMillis() - start)
            + "ms; region information: " + getRegionDebugInfoSafe(table, put.getRow())
            + "; errors: " + exceptionInfo);
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
  }

  @Override
  public void waitForFinish() {
    super.waitForFinish();
    System.out.println("Failed to write keys: " + failedKeySet.size());
    for (Long key : failedKeySet) {
       System.out.println("Failed to write key: " + key);
    }
  }
}
