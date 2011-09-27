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
package org.apache.hadoop.hbase.manual.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.manual.HBaseTest;
import org.apache.hadoop.hbase.util.Bytes;

public class MultiThreadedReader extends MultiThreadedAction
{
  private static final Log LOG = LogFactory.getLog(MultiThreadedReader.class);
  Set<HBaseReader> readers_ = new HashSet<HBaseReader>();

  public MultiThreadedReader(HBaseConfiguration conf, byte[] tableName, byte[] columnFamily) {
    tableName_ = tableName;
    columnFamily_ = columnFamily;
    conf_ = conf;
  }

  public void setVerficationPercent(float verifyPercent) {
    verifyPercent_ = verifyPercent;
  }

  public void start(long startKey, long endKey, int numThreads) {
    if(verbose_) {
      LOG.debug("Inserting keys [" + startKey + ", " + endKey + ")");
    }
    startKey_ = startKey;
    endKey_ = endKey;
    numThreads_ = numThreads;

    long threadStartKey = startKey;
    long threadEndKey = startKey;
    for(int i = 0; i < numThreads_; ++i) {
      threadStartKey = (startKey == -1) ? -1 : threadEndKey ;
      threadEndKey = startKey + (i+1) * (endKey - startKey) / numThreads_;
      HBaseReader reader = new HBaseReader(this, i, threadStartKey, threadEndKey);
      readers_.add(reader);
    }
    numThreadsWorking_.addAndGet(readers_.size());
    for(HBaseReader reader : readers_) {
      reader.start();
    }

    startReporter("R");
  }

  public static class HBaseReader extends Thread {
    int id_;
    MultiThreadedReader reader_;
    List<HTable> tables_ = new ArrayList<HTable>();
    long startKey_;
    long endKey_;
    static int minDataSize_ = 256;
    static int maxDataSize_ = 1024;
    static DataGenerator dataGenerator_ = new DataGenerator(minDataSize_, maxDataSize_);

    public HBaseReader(MultiThreadedReader reader, int id, long startKey, long endKey) {
      id_ = id;
      reader_ = reader;
      HTable table = HBaseUtils.getHTable(reader_.conf_, tableName_);
      tables_.add(table);
      startKey_ = startKey;
      endKey_ = endKey;
    }

    public void run() {
      if (reader_.verbose_) {
        LOG.info("Started thread #" + id_ + " for reads...");
      }
      boolean repeatQuery = false;
      Get get = null;
      long start = 0;
      long curKey = 0;

      for(;;) {
        if(!repeatQuery) {

          if (startKey_ == -1) {
            // load test is running at the same time.
            if (MultiThreadedWriter.insertedKeySet_.size() > 0) {
              int idx = reader_.random_.nextInt(MultiThreadedWriter.insertedKeySet_.size());
              curKey = MultiThreadedWriter.insertedKeySet_.get(idx);
            } else {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
              }
            }
          } else {
            curKey = startKey_ + Math.abs(reader_.random_.nextLong())%(endKey_ - startKey_);
          }
          get = new Get(DataGenerator.paddedKey(curKey).getBytes());
          get.addFamily(columnFamily_);
          // get.addColumn(columnFamily_, Bytes.toBytes("0"));
        }
        repeatQuery = false;
        try {
          if(reader_.verbose_ && repeatQuery) {
            LOG.info("[" + id_ + "] " + (repeatQuery?"RE-Querying":"Querying") + " key  = " + curKey + ", cf = " + new String(columnFamily_));
          }
          queryKey( get, (reader_.random_.nextInt(100) < reader_.verifyPercent_) );
        }
        catch (IOException e) {
          reader_.numOpFailures_.addAndGet(1);
          LOG.debug("[" + id_ + "] FAILED read, key = " + (curKey + "") + ", time = " + (System.currentTimeMillis() - start) + " ms");
          repeatQuery = true;
        }
      }
    }

    public void queryKey(Get get, boolean verify) throws IOException {
      String rowKey = new String(get.getRow());
      for(HTable table : tables_) {
//        if(verbose_) {
//          HRegionLocation hloc = table.getRegionLocation(Bytes.toBytes(rowKey));
//          LOG.info("Key = " + rowKey + ", RegoinServer: " + hloc.getServerAddress().getHostname());
//        }
        // read the data
        long start = System.currentTimeMillis();
        Result result = table.get(get);
        reader_.cumulativeOpTime_.addAndGet(System.currentTimeMillis() - start);
        reader_.numKeys_.addAndGet(1);

        // if we got no data report error
        if(result.isEmpty()) {
           HRegionLocation hloc = table.getRegionLocation(Bytes.toBytes(rowKey));
           LOG.info("Key = " + rowKey + ", RegionServer: " + hloc.getServerAddress().getHostname());
           reader_.numErrors_.addAndGet(1);
           LOG.error("No data returned, tried to get actions for key = " + rowKey);

           if (reader_.numErrors_.get() > 3) {
             LOG.error("Aborting run -- found more than three errors\n");
             System.exit(-1);
           }
        }

        if(result.getFamilyMap(columnFamily_) != null) {

          // increment number of columns read
          reader_.numCols_.addAndGet(result.getFamilyMap(columnFamily_).size());

          if (verify) {
            // verify the result
            List<KeyValue> keyValues = result.list();
            for(KeyValue kv : keyValues) {
              String actionId = new String(kv.getQualifier());
              String data = new String(kv.getValue());

              // if something does not look right report it
              if(!DataGenerator.verify(rowKey, actionId, data)) {
                reader_.numErrors_.addAndGet(1);
                LOG.error("Error checking data for key = " + rowKey + ", actionId = " + actionId);
              }
            }

            reader_.numKeysVerified_.addAndGet(1);
          }
        }
      }
    }
  }
}
