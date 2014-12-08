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
package org.apache.hadoop.hbase.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

/**
 * Test Map/Reduce job over HBase tables. The map/reduce process we're testing
 * on our tables is simple - take every row in the table, reverse the value of
 * a particular cell, and write it back to the table.
 */
@Category(LargeTests.class)
public class TestMultithreadedTableMapper {
  private static final Log LOG = LogFactory.getLog(TestMultithreadedTableMapper.class);
  private static final HBaseTestingUtility UTIL =
      new HBaseTestingUtility();
  static final byte[] MULTI_REGION_TABLE_NAME = Bytes.toBytes("mrtest");
  static final byte[] INPUT_FAMILY = Bytes.toBytes("contents");
  static final byte[] OUTPUT_FAMILY = Bytes.toBytes("text");
  static final int    NUMBER_OF_THREADS = 10;

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
    HTable table = UTIL.createTable(MULTI_REGION_TABLE_NAME, new byte[][] {INPUT_FAMILY, OUTPUT_FAMILY});
    UTIL.createMultiRegions(table, INPUT_FAMILY);
    UTIL.loadTable(table, INPUT_FAMILY, false);
    UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniMapReduceCluster();
    UTIL.shutdownMiniCluster();
  }

  /**
   * Pass the given key and processed record reduce
   */
  public static class ProcessContentsMapper
  extends TableMapper<ImmutableBytesWritable, Put> {

    /**
     * Pass the key, and reversed value to reduce
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     */
    public void map(ImmutableBytesWritable key, Result value,
        Context context)
            throws IOException, InterruptedException {
      if (value.size() != 1) {
        throw new IOException("There should only be one input column");
      }
      Map<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
      cf = value.getMap();
      if(!cf.containsKey(INPUT_FAMILY)) {
        throw new IOException("Wrong input columns. Missing: '" +
            Bytes.toString(INPUT_FAMILY) + "'.");
      }
      // Get the original value and reverse it
      String originalValue = Bytes.toString(value.getValue(INPUT_FAMILY, null));
      StringBuilder newValue = new StringBuilder(originalValue);
      newValue.reverse();
      // Now set the value to be collected
      Put outval = new Put(key.get());
      outval.add(OUTPUT_FAMILY, null, Bytes.toBytes(newValue.toString()));
      context.write(key, outval);
    }
  }

  /**
   * Test multithreadedTableMappper map/reduce against a multi-region table
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testMultithreadedTableMapper()
      throws IOException, InterruptedException, ClassNotFoundException {
    runTestOnTable(new HTable(new Configuration(UTIL.getConfiguration()),
        MULTI_REGION_TABLE_NAME));
  }

  private void runTestOnTable(HTable table)
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job = null;
    try {
      LOG.info("Before map/reduce startup");
      job = new Job(table.getConfiguration(), "process column contents");
      job.setNumReduceTasks(1);
      Scan scan = new Scan();
      scan.addFamily(INPUT_FAMILY);
      TableMapReduceUtil.initTableMapperJob(
          Bytes.toString(table.getTableName()), scan,
          MultithreadedTableMapper.class, ImmutableBytesWritable.class,
          Put.class, job);
      MultithreadedTableMapper.setMapperClass(job, ProcessContentsMapper.class);
      MultithreadedTableMapper.setNumberOfThreads(job, NUMBER_OF_THREADS);
      TableMapReduceUtil.initTableReducerJob(
          Bytes.toString(table.getTableName()),
          IdentityTableReducer.class, job);
      FileOutputFormat.setOutputPath(job, new Path("test"));
      LOG.info("Started " + Bytes.toString(table.getTableName()));
      assertTrue(job.waitForCompletion(true));
      LOG.info("After map/reduce completion");
      // verify map-reduce results
      verify(Bytes.toString(table.getTableName()));
    } finally {
      table.close();
      if (job != null) {
        FileUtil.fullyDelete(
            new File(job.getConfiguration().get("hadoop.tmp.dir")));
      }
    }
  }

  private void verify(String tableName) throws IOException {
    HTable table = new HTable(new Configuration(UTIL.getConfiguration()), tableName);
    boolean verified = false;
    long pause = UTIL.getConfiguration().getLong("hbase.client.pause", 5 * 1000);
    int numRetries = UTIL.getConfiguration().getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5);
    for (int i = 0; i < numRetries; i++) {
      try {
        LOG.info("Verification attempt #" + i);
        verifyAttempt(table);
        verified = true;
        break;
      } catch (NullPointerException e) {
        // If here, a cell was empty.  Presume its because updates came in
        // after the scanner had been opened.  Wait a while and retry.
        LOG.debug("Verification attempt failed: " + e.getMessage());
      }
      try {
        Thread.sleep(pause);
      } catch (InterruptedException e) {
        // continue
      }
    }
    assertTrue(verified);
    table.close();
  }

  /**
   * Looks at every value of the mapreduce output and verifies that indeed
   * the values have been reversed.
   *
   * @param table Table to scan.
   * @throws IOException
   * @throws NullPointerException if we failed to find a cell value
   */
  private void verifyAttempt(final HTable table)
      throws IOException, NullPointerException {
    Scan scan = new Scan();
    scan.addFamily(INPUT_FAMILY);
    scan.addFamily(OUTPUT_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    try {
      Iterator<Result> itr = scanner.iterator();
      assertTrue(itr.hasNext());
      while(itr.hasNext()) {
        Result r = itr.next();
        if (LOG.isDebugEnabled()) {
          if (r.size() > 2 ) {
            throw new IOException("Too many results, expected 2 got " +
                r.size());
          }
        }
        byte[] firstValue = null;
        byte[] secondValue = null;
        int count = 0;
        for(Cell kv : r.listCells()) {
          if (count == 0) {
            firstValue = CellUtil.cloneValue(kv);
          }else if (count == 1) {
            secondValue = CellUtil.cloneValue(kv);
          }else if (count == 2) {
            break;
          }
          count++;
        }
        String first = "";
        if (firstValue == null) {
          throw new NullPointerException(Bytes.toString(r.getRow()) +
              ": first value is null");
        }
        first = Bytes.toString(firstValue);
        String second = "";
        if (secondValue == null) {
          throw new NullPointerException(Bytes.toString(r.getRow()) +
              ": second value is null");
        }
        byte[] secondReversed = new byte[secondValue.length];
        for (int i = 0, j = secondValue.length - 1; j >= 0; j--, i++) {
          secondReversed[i] = secondValue[j];
        }
        second = Bytes.toString(secondReversed);
        if (first.compareTo(second) != 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("second key is not the reverse of first. row=" +
                Bytes.toStringBinary(r.getRow()) + ", first value=" + first +
                ", second value=" + second);
          }
          fail();
        }
      }
    } finally {
      scanner.close();
    }
  }

}

