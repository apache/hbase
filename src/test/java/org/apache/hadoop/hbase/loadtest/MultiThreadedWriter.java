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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HasThread;

public class MultiThreadedWriter extends MultiThreadedAction
{
  private static final Log LOG = LogFactory.getLog(MultiThreadedWriter.class);
  Set<HBaseWriter> writers_ = new HashSet<HBaseWriter>();

  /* This is the current key to be inserted by any thread. Each thread does an 
     atomic get and increment operation and inserts the current value. */
  public static AtomicLong currentKey_ = null;
  /* The sorted set of keys inserted by the writers */
  public static List<Long> insertedKeySet_ = Collections.synchronizedList(
      new ArrayList<Long>());
  /* The sorted set of keys NOT inserted by the writers */
  public static List<Long> failedKeySet_ = Collections.synchronizedList(
      new ArrayList<Long>());
  public boolean bulkLoad = true;


  public MultiThreadedWriter(Configuration config, byte[] tableName) {
    this.tableName = tableName;
    this.conf = config;
  }

  public void start(long startKey, long endKey, int numThreads) {
    this.startKey = startKey;
    this.endKey = endKey;
    this.numThreads = numThreads;
    currentKey_ = new AtomicLong(this.startKey);

    if(getVerbose()) {
      LOG.debug("Inserting keys [" + this.startKey + ", " + this.endKey + ")");
    }

    for(int i = 0; i < this.numThreads; ++i) {
      HBaseWriter writer = new HBaseWriter(this, i);
      writers_.add(writer);
    }
    numThreadsWorking.addAndGet(writers_.size());
    for(HBaseWriter writer : writers_) {
      writer.start();
    }
    startReporter("W");
  }

  
  public void setBulkLoad(boolean bulkLoad) {
    this.bulkLoad = bulkLoad;
  }

  public boolean getBulkLoad() {
    return bulkLoad;
  }

  public void waitForFinish() {
    while(numThreadsWorking.get() != 0) {
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    System.out.println("Failed Key Count: " + failedKeySet_.size());
    for (Long key : failedKeySet_) {
      System.out.println("Failure for key: " + key);
    }

  }

  /**
   * This is an unsafe operation so avoid use.
   */
  public void killAllThreads() {
    for (HBaseWriter writer: writers_) {
      if (writer != null && writer.isAlive()) {
        writer.stop();
      }
    }
    super.killAllThreads();
  }

  public static class HBaseWriter extends Thread {
    int id_;
    MultiThreadedWriter writer_;
    Random random_ = new Random();
    List<HTable> tables_ = new ArrayList<HTable>();

    public HBaseWriter(MultiThreadedWriter writer, int id) {
      id_ = id;
      this.writer_ = writer;
      HTable table = HBaseUtils.getHTable(this.writer_.conf, tableName);
      tables_.add(table);
    }

    public void run() {
      long rowKey = currentKey_.getAndIncrement();
      do {
        if (writer_.getVerbose()) {
          //LOG.info("Writing key: "+rowKey);
        }
        if (writer_.bulkLoad == true) {
          bulkInsertKey(rowKey, writer_.getColumnFamilyProperties());
        } else {
          insertKeys(rowKey, writer_.getColumnFamilyProperties());
        }
        rowKey = currentKey_.getAndIncrement();
      } while(writer_.shouldContinueRunning() && rowKey < writer_.endKey);
      writer_.numThreadsWorking.decrementAndGet();
    }

    public void bulkInsertKey(long rowKey,
        ColumnFamilyProperties[] familyProperties) {
      Put put = DataGenerator.getPut(rowKey, familyProperties);
      try {
        long start = System.currentTimeMillis();
        putIntoTables(put);
        insertedKeySet_.add(rowKey);
        writer_.numRows_.addAndGet(1);
        writer_.numKeys_.addAndGet(put.size());
        writer_.cumulativeOpTime_.addAndGet(System.currentTimeMillis() - start);
      }
      catch (IOException e) {
        writer_.numOpFailures_.addAndGet(1);
        failedKeySet_.add(rowKey);
        e.printStackTrace();
      }
    }

    public void insertKeys(long rowKey,
        ColumnFamilyProperties[] familyProperties) {
      byte[] row = RegionSplitter.getHBaseKeyFromRowID(rowKey);
      //LOG.info("Inserting row: "+Bytes.toString(row)); 
      int insertedSize = 0;
      try {
        long start = System.currentTimeMillis();
        for (ColumnFamilyProperties familyProperty : familyProperties) {
          TreeSet<KeyValue> kvSet = DataGenerator.
          getSortedResultSet(rowKey, familyProperty);
          for (KeyValue kv: kvSet) {
            Put put = new Put(row);
            put.add(kv);
            insertedSize ++;
            putIntoTables(put);
          }
        }
        insertedKeySet_.add(rowKey);
        writer_.numRows_.addAndGet(1);
        writer_.numKeys_.addAndGet(insertedSize);
        writer_.cumulativeOpTime_.addAndGet(System.currentTimeMillis() - start);
      }
      catch (IOException e) {
        writer_.numOpFailures_.addAndGet(1);
        failedKeySet_.add(rowKey);
        e.printStackTrace();
      }
    }

    public void putIntoTables(Put put) throws IOException {
      for(HTable table : tables_) {
        table.put(put);
      }
    }
  }
}
