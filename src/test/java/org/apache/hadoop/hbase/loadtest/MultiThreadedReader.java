/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.loadtest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HasThread;

public class MultiThreadedReader extends MultiThreadedAction {
  private static final Log LOG = LogFactory.getLog(MultiThreadedReader.class);
  Set<HBaseReader> readers_ = new HashSet<HBaseReader>();
  private boolean writesHappeningInParallel_ = false;

  public void setWriteHappeningInParallel() {
    writesHappeningInParallel_ = true;
  }

  public boolean areWritesHappeningInParallel() {
    return writesHappeningInParallel_;
  }

  public MultiThreadedReader(Configuration config, byte[] tableName) {
    this.tableName = tableName;
    this.conf = config;
  }

  public void start(long startKey, long endKey, int numThreads) {
    this.startKey = startKey;
    this.endKey = endKey;
    this.numThreads = numThreads;

    if (this.getVerbose()) {
      LOG.info("Reading keys [" + this.startKey + ", " + this.endKey + ")");
    }

    long threadStartKey = this.startKey;
    long threadEndKey = this.startKey;
    for (int i = 0; i < this.numThreads; ++i) {
      threadStartKey = (this.startKey == -1) ? -1 : threadEndKey;
      threadEndKey = this.startKey + (i + 1) * (this.endKey - this.startKey)
          / this.numThreads;
      HBaseReader reader = new HBaseReader(this, i, threadStartKey,
          threadEndKey);
      readers_.add(reader);
    }
    numThreadsWorking.addAndGet(readers_.size());
    for (HBaseReader reader : readers_) {
      reader.start();
    }
    startReporter("R");
  }

  /**
   * This is an unsafe operation so avoid use.
   */
  public void killAllThreads() {
    for (HBaseReader reader : readers_) {
      if (reader != null && reader.isAlive()) {
        reader.stop();
      }
    }
    super.killAllThreads();
  }

  public static class HBaseReader extends Thread {
    int id_;
    MultiThreadedReader reader_;
    List<HTable> tables_ = new ArrayList<HTable>();
    long startKey_;
    long endKey_;

    public HBaseReader(MultiThreadedReader reader, int id, long startKey,
        long endKey) {
      id_ = id;
      reader_ = reader;
      HTable table = HBaseUtils.getHTable(reader_.conf, tableName);
      tables_.add(table);
      startKey_ = startKey;
      endKey_ = endKey;
    }

    public void run() {
      if (reader_.getVerbose()) {
        // LOG.info("Started thread #" + id_ + " for reads...");
      }
      boolean repeatQuery = false;
      long start = 0;
      long curKey = 0;

      while (reader_.shouldContinueRunning()) {
        if (!repeatQuery) {
          if (reader_.areWritesHappeningInParallel()) {
            // load test is running at the same time.
            while (MultiThreadedWriter.insertedKeySet_.size() <= 0) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
              }
            }
            int idx = reader_.random
                .nextInt(MultiThreadedWriter.insertedKeySet_.size());
            curKey = MultiThreadedWriter.insertedKeySet_.get(idx);
          } else {
            curKey = startKey_ + Math.abs(reader_.random.nextLong())
                % (endKey_ - startKey_);
          }
        } else {
          repeatQuery = false;
        }
        try {
          if (reader_.getVerbose() && repeatQuery) {
            LOG.info("[" + id_ + "] "
                + (repeatQuery ? "RE-Querying" : "Querying") + " key  = "
                + curKey);
          }
          queryKey(curKey,
              (reader_.random.nextInt(100) < reader_.verifyPercent));
        } catch (IOException e) {
          reader_.numOpFailures_.addAndGet(1);
          LOG.debug("[" + id_ + "] FAILED read, key = " + (curKey + "") + ","
              + "time = " + (System.currentTimeMillis() - start) + " ms");
          repeatQuery = true;
        }
      }
      reader_.numThreadsWorking.decrementAndGet();
    }

    public void queryKey(long rowKey, boolean verify) throws IOException {
      for (HTable table : tables_) {
        for (ColumnFamilyProperties familyProperty : reader_
            .getColumnFamilyProperties()) {
          //Get get = new Get(DataGenerator.paddedKey(rowKey).getBytes());
          Get get = new Get(RegionSplitter.getHBaseKeyFromRowID(rowKey));
          get.setMaxVersions(familyProperty.maxVersions);
          get.addFamily(Bytes.toBytes(familyProperty.familyName));
          Filter filter = DataGenerator.getFilter(rowKey, familyProperty);
          get.setFilter(filter);
          long start = System.currentTimeMillis();
          Result result = table.get(get);
          reader_.cumulativeOpTime_.addAndGet(System.currentTimeMillis()
              - start);
          reader_.numRows_.addAndGet(1);
          reader_.numKeys_.addAndGet(result.size());
          if (verify) {
            KeyValue[] kvResult = result.raw();
            TreeSet<KeyValue> kvExpectedFull = DataGenerator
                .getSortedResultSet(rowKey, familyProperty);
            TreeSet<KeyValue> kvExpectedFiltered = DataGenerator
                .filterAndVersioningForSingleRowFamily(kvExpectedFull, filter,
                    familyProperty.maxVersions);
            boolean verificationResult = verifyResultSetIgnoringDuplicates(
                kvResult, kvExpectedFiltered);
            if (verificationResult == false) {
              reader_.numErrors_.addAndGet(1);
              LOG.error("Error checking data for key = " + rowKey);
              if (reader_.numErrors_.get() > 1) {
                LOG.error("Aborting run -- found more than one error\n");
                if (reader_.getStopOnError()) {
                  System.exit(-1);
                }
              }
            }
            reader_.numRowsVerified_.addAndGet(1);
          }
        }
      }
    }

    boolean verifyResultSet(KeyValue[] kvResult, TreeSet<KeyValue> kvExpected) {
      if (kvResult.length != kvExpected.size()) {
        LOG.info("Expected size was: " + kvExpected.size() + " "
            + "but result set was of size: " + kvResult.length);
        return false;
      }
      int index = 0;
      for (KeyValue kv : kvExpected) {
        if (KeyValue.COMPARATOR.compare(kv, kvResult[index++]) != 0) {
          return false;
        }
      }
      return true;
    }

    boolean verifyResultSetIgnoringDuplicates(KeyValue[] kvResult,
        TreeSet<KeyValue> kvExpected) {
      TreeSet<KeyValue> noDuplicateResultSet = new TreeSet<KeyValue>(
          new KeyValue.KVComparator());
      for (KeyValue kv : kvResult) {
        noDuplicateResultSet.add(kv);
      }
      boolean isCorrect = noDuplicateResultSet.equals(kvExpected);

      if (isCorrect == false) {
        debugMismatchResults(noDuplicateResultSet, kvExpected);
      }
      return isCorrect;
    }
  }

  static void debugMismatchResults(TreeSet<KeyValue> noDuplicateResultSet,
      TreeSet<KeyValue> kvExpected) {
    if (noDuplicateResultSet.size() != kvExpected.size()) {
      LOG.info("Expected size was: " + kvExpected.size()
          + " but result set was of size: " + noDuplicateResultSet.size());
    }
    Iterator<KeyValue> expectedIterator = kvExpected.iterator();
    Iterator<KeyValue> returnedIterator = noDuplicateResultSet.iterator();
    while (expectedIterator.hasNext() || returnedIterator.hasNext()) {
      KeyValue expected = null;
      KeyValue returned = null;
      if (expectedIterator.hasNext()) {
        expected = expectedIterator.next();
      }
      if (returnedIterator.hasNext()) {
        returned = returnedIterator.next();
      }
      if (returned == null || expected == null) {
        LOG.info("MISMATCH!! Expected was : " + expected + " but got "
            + returned);
      } else if (KeyValue.COMPARATOR.compare(expected, returned) != 0) {
        LOG.info("MISMATCH!! Expected was : " + expected + " but got "
            + returned);
      } else {
        LOG.info("Expected was : " + expected + " and got " + returned);
      }
    }
  }
}
