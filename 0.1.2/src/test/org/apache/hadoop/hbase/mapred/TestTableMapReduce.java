/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase.mapred;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HScannerInterface;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MultiRegionTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.OutputCollector;

/**
 * Test Map/Reduce job over HBase tables
 */
public class TestTableMapReduce extends MultiRegionTable {
  @SuppressWarnings("hiding")
  private static final Log LOG =
    LogFactory.getLog(TestTableMapReduce.class.getName());

  static final String MULTI_REGION_TABLE_NAME = "mrtest";
  static final String INPUT_COLUMN = "contents:";
  static final Text TEXT_INPUT_COLUMN = new Text(INPUT_COLUMN);
  static final String OUTPUT_COLUMN = "text:";
  static final Text TEXT_OUTPUT_COLUMN = new Text(OUTPUT_COLUMN);
  
  private static final Text[] columns = {
    TEXT_INPUT_COLUMN,
    TEXT_OUTPUT_COLUMN
  };

  private MiniHBaseCluster hCluster = null;
  
  /** constructor */
  public TestTableMapReduce() {
    super(INPUT_COLUMN);
    desc = new HTableDescriptor(MULTI_REGION_TABLE_NAME);
    desc.addFamily(new HColumnDescriptor(INPUT_COLUMN));
    desc.addFamily(new HColumnDescriptor(OUTPUT_COLUMN));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // Start up HBase cluster
    // Only one region server.  MultiRegionServer manufacturing code below
    // depends on there being one region server only.
    hCluster = new MiniHBaseCluster(conf, 1, dfsCluster, true);
    LOG.info("Master is at " + this.conf.get(HConstants.MASTER_ADDRESS));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void tearDown() throws Exception {
    if(hCluster != null) {
      hCluster.shutdown();
    }
    super.tearDown();
  }

  /**
   * Pass the given key and processed record reduce
   */
  public static class ProcessContentsMapper extends TableMap<Text, MapWritable> {
    /**
     * Pass the key, and reversed value to reduce
     *
     * @see org.apache.hadoop.hbase.mapred.TableMap#map(org.apache.hadoop.hbase.HStoreKey, org.apache.hadoop.io.MapWritable, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void map(HStoreKey key, MapWritable value,
        OutputCollector<Text, MapWritable> output,
        @SuppressWarnings("unused") Reporter reporter) throws IOException {
      
      Text tKey = key.getRow();
      
      if(value.size() != 1) {
        throw new IOException("There should only be one input column");
      }

      Text[] keys = value.keySet().toArray(new Text[value.size()]);
      if(!keys[0].equals(TEXT_INPUT_COLUMN)) {
        throw new IOException("Wrong input column. Expected: " + INPUT_COLUMN
            + " but got: " + keys[0]);
      }

      // Get the original value and reverse it
      
      String originalValue =
        new String(((ImmutableBytesWritable)value.get(keys[0])).get(),
            HConstants.UTF8_ENCODING);
      StringBuilder newValue = new StringBuilder();
      for(int i = originalValue.length() - 1; i >= 0; i--) {
        newValue.append(originalValue.charAt(i));
      }
      
      // Now set the value to be collected

      MapWritable outval = new MapWritable();
      outval.put(TEXT_OUTPUT_COLUMN, new ImmutableBytesWritable(
          newValue.toString().getBytes(HConstants.UTF8_ENCODING)));
      
      output.collect(tKey, outval);
    }
  }
  
  /**
   * Test hbase mapreduce jobs against a multi-region table.
   * @throws IOException
   */
  public void testTableMapReduce() throws IOException {
    @SuppressWarnings("deprecation")
    MiniMRCluster mrCluster = new MiniMRCluster(2, fs.getUri().toString(), 1);

    try {
      JobConf jobConf = new JobConf(conf, TestTableMapReduce.class);
      jobConf.setJobName("process column contents");
      jobConf.setNumMapTasks(2);
      jobConf.setNumReduceTasks(1);

      TableMap.initJob(MULTI_REGION_TABLE_NAME, INPUT_COLUMN, 
          ProcessContentsMapper.class, jobConf);

      TableReduce.initJob(MULTI_REGION_TABLE_NAME,
          IdentityTableReduce.class, jobConf);
      LOG.info("Started " + MULTI_REGION_TABLE_NAME);
      JobClient.runJob(jobConf);

      // verify map-reduce results
      verify(MULTI_REGION_TABLE_NAME);
    } finally {
      mrCluster.shutdown();
    }
  }

  @SuppressWarnings("null")
  private void verify(String tableName) throws IOException {
    HTable table = new HTable(conf, new Text(tableName));
    boolean verified = false;
    long pause = conf.getLong("hbase.client.pause", 5 * 1000);
    int numRetries = conf.getInt("hbase.client.retries.number", 5);
    for (int i = 0; i < numRetries; i++) {
      try {
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
  }

  /**
   * Looks at every value of the mapreduce output and verifies that indeed
   * the values have been reversed.
   * @param table Table to scan.
   * @throws IOException
   * @throws NullPointerException if we failed to find a cell value
   */
  private void verifyAttempt(final HTable table) throws IOException, NullPointerException {
    HScannerInterface scanner =
      table.obtainScanner(columns, HConstants.EMPTY_START_ROW);
    try {
      HStoreKey key = new HStoreKey();
      TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
      
      while(scanner.next(key, results)) {
        if (LOG.isDebugEnabled()) {
          if (results.size() > 2 ) {
            throw new IOException("Too many results, expected 2 got " +
              results.size());
          }
        }
        byte[] firstValue = null;
        byte[] secondValue = null;
        int count = 0;
        for(Map.Entry<Text, byte[]> e: results.entrySet()) {
          if (count == 0) {
            firstValue = e.getValue();
          }
          if (count == 1) {
            secondValue = e.getValue();
          }
          count++;
          if (count == 2) {
            break;
          }
        }
        
        String first = "";
        if (firstValue == null) {
          throw new NullPointerException(key.getRow().toString() +
            ": first value is null");
        }
        first = new String(firstValue, HConstants.UTF8_ENCODING);
        
        String second = "";
        if (secondValue == null) {
          throw new NullPointerException(key.getRow().toString() +
            ": second value is null");
        }
        byte[] secondReversed = new byte[secondValue.length];
        for (int i = 0, j = secondValue.length - 1; j >= 0; j--, i++) {
          secondReversed[i] = secondValue[j];
        }
        second = new String(secondReversed, HConstants.UTF8_ENCODING);

        if (first.compareTo(second) != 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("second key is not the reverse of first. row=" +
                key.getRow() + ", first value=" + first + ", second value=" +
                second);
          }
          fail();
        }
      }
    } finally {
      scanner.close();
    }
  }
}