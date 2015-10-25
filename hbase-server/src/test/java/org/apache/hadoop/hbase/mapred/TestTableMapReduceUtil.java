/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

@Category({MapReduceTests.class, LargeTests.class})
public class TestTableMapReduceUtil {

  private static final Log LOG = LogFactory
      .getLog(TestTableMapReduceUtil.class);

  private static Table presidentsTable;
  private static final String TABLE_NAME = "People";

  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("info");
  private static final byte[] COLUMN_QUALIFIER = Bytes.toBytes("name");

  private static ImmutableSet<String> presidentsRowKeys = ImmutableSet.of(
      "president1", "president2", "president3");
  private static Iterator<String> presidentNames = ImmutableSet.of(
      "John F. Kennedy", "George W. Bush", "Barack Obama").iterator();

  private static ImmutableSet<String> actorsRowKeys = ImmutableSet.of("actor1",
      "actor2");
  private static Iterator<String> actorNames = ImmutableSet.of(
      "Jack Nicholson", "Martin Freeman").iterator();

  private static String PRESIDENT_PATTERN = "president";
  private static String ACTOR_PATTERN = "actor";
  private static ImmutableMap<String, ImmutableSet<String>> relation = ImmutableMap
      .of(PRESIDENT_PATTERN, presidentsRowKeys, ACTOR_PATTERN, actorsRowKeys);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
    presidentsTable = createAndFillTable(TableName.valueOf(TABLE_NAME));
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws IOException {
    LOG.info("before");
    UTIL.ensureSomeRegionServersAvailable(1);
    LOG.info("before done");
  }

  public static Table createAndFillTable(TableName tableName) throws IOException {
    Table table = UTIL.createTable(tableName, COLUMN_FAMILY);
    createPutCommand(table);
    return table;
  }

  private static void createPutCommand(Table table) throws IOException {
    for (String president : presidentsRowKeys) {
      if (presidentNames.hasNext()) {
        Put p = new Put(Bytes.toBytes(president));
        p.addColumn(COLUMN_FAMILY, COLUMN_QUALIFIER, Bytes.toBytes(presidentNames.next()));
        table.put(p);
      }
    }

    for (String actor : actorsRowKeys) {
      if (actorNames.hasNext()) {
        Put p = new Put(Bytes.toBytes(actor));
        p.addColumn(COLUMN_FAMILY, COLUMN_QUALIFIER, Bytes.toBytes(actorNames.next()));
        table.put(p);
      }
    }
  }

  /**
   * Check what the given number of reduce tasks for the given job configuration
   * does not exceed the number of regions for the given table.
   */
  @Test
  public void shouldNumberOfReduceTaskNotExceedNumberOfRegionsForGivenTable()
      throws IOException {
    Assert.assertNotNull(presidentsTable);
    Configuration cfg = UTIL.getConfiguration();
    JobConf jobConf = new JobConf(cfg);
    TableMapReduceUtil.setNumReduceTasks(TABLE_NAME, jobConf);
    TableMapReduceUtil.limitNumReduceTasks(TABLE_NAME, jobConf);
    TableMapReduceUtil.setScannerCaching(jobConf, 100);
    assertEquals(1, jobConf.getNumReduceTasks());
    assertEquals(100, jobConf.getInt("hbase.client.scanner.caching", 0));

    jobConf.setNumReduceTasks(10);
    TableMapReduceUtil.setNumMapTasks(TABLE_NAME, jobConf);
    TableMapReduceUtil.limitNumReduceTasks(TABLE_NAME, jobConf);
    assertEquals(1, jobConf.getNumReduceTasks());
  }

  @Test
  public void shouldNumberOfMapTaskNotExceedNumberOfRegionsForGivenTable()
      throws IOException {
    Configuration cfg = UTIL.getConfiguration();
    JobConf jobConf = new JobConf(cfg);
    TableMapReduceUtil.setNumReduceTasks(TABLE_NAME, jobConf);
    TableMapReduceUtil.limitNumMapTasks(TABLE_NAME, jobConf);
    assertEquals(1, jobConf.getNumMapTasks());

    jobConf.setNumMapTasks(10);
    TableMapReduceUtil.setNumMapTasks(TABLE_NAME, jobConf);
    TableMapReduceUtil.limitNumMapTasks(TABLE_NAME, jobConf);
    assertEquals(1, jobConf.getNumMapTasks());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void shoudBeValidMapReduceEvaluation() throws Exception {
    Configuration cfg = UTIL.getConfiguration();
    JobConf jobConf = new JobConf(cfg);
    try {
      jobConf.setJobName("process row task");
      jobConf.setNumReduceTasks(1);
      TableMapReduceUtil.initTableMapJob(TABLE_NAME, new String(COLUMN_FAMILY),
          ClassificatorMapper.class, ImmutableBytesWritable.class, Put.class,
          jobConf);
      TableMapReduceUtil.initTableReduceJob(TABLE_NAME,
          ClassificatorRowReduce.class, jobConf);
      RunningJob job = JobClient.runJob(jobConf);
      assertTrue(job.isSuccessful());
    } finally {
      if (jobConf != null)
        FileUtil.fullyDelete(new File(jobConf.get("hadoop.tmp.dir")));
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void shoudBeValidMapReduceWithPartitionerEvaluation()
      throws IOException {
    Configuration cfg = UTIL.getConfiguration();
    JobConf jobConf = new JobConf(cfg);
    try {
      jobConf.setJobName("process row task");
      jobConf.setNumReduceTasks(2);
      TableMapReduceUtil.initTableMapJob(TABLE_NAME, new String(COLUMN_FAMILY),
          ClassificatorMapper.class, ImmutableBytesWritable.class, Put.class,
          jobConf);

      TableMapReduceUtil.initTableReduceJob(TABLE_NAME,
          ClassificatorRowReduce.class, jobConf, HRegionPartitioner.class);
      RunningJob job = JobClient.runJob(jobConf);
      assertTrue(job.isSuccessful());
    } finally {
      if (jobConf != null)
        FileUtil.fullyDelete(new File(jobConf.get("hadoop.tmp.dir")));
    }
  }

  @SuppressWarnings("deprecation")
  static class ClassificatorRowReduce extends MapReduceBase implements
      TableReduce<ImmutableBytesWritable, Put> {

    @Override
    public void reduce(ImmutableBytesWritable key, Iterator<Put> values,
        OutputCollector<ImmutableBytesWritable, Put> output, Reporter reporter)
        throws IOException {
      String strKey = Bytes.toString(key.get());
      List<Put> result = new ArrayList<Put>();
      while (values.hasNext())
        result.add(values.next());

      if (relation.keySet().contains(strKey)) {
        Set<String> set = relation.get(strKey);
        if (set != null) {
          assertEquals(set.size(), result.size());
        } else {
          throwAccertionError("Test infrastructure error: set is null");
        }
      } else {
        throwAccertionError("Test infrastructure error: key not found in map");
      }
    }

    private void throwAccertionError(String errorMessage) throws AssertionError {
      throw new AssertionError(errorMessage);
    }
  }

  @SuppressWarnings("deprecation")
  static class ClassificatorMapper extends MapReduceBase implements
      TableMap<ImmutableBytesWritable, Put> {

    @Override
    public void map(ImmutableBytesWritable row, Result result,
        OutputCollector<ImmutableBytesWritable, Put> outCollector,
        Reporter reporter) throws IOException {
      String rowKey = Bytes.toString(result.getRow());
      final ImmutableBytesWritable pKey = new ImmutableBytesWritable(
          Bytes.toBytes(PRESIDENT_PATTERN));
      final ImmutableBytesWritable aKey = new ImmutableBytesWritable(
          Bytes.toBytes(ACTOR_PATTERN));
      ImmutableBytesWritable outKey = null;

      if (rowKey.startsWith(PRESIDENT_PATTERN)) {
        outKey = pKey;
      } else if (rowKey.startsWith(ACTOR_PATTERN)) {
        outKey = aKey;
      } else {
        throw new AssertionError("unexpected rowKey");
      }

      String name = Bytes.toString(result.getValue(COLUMN_FAMILY,
          COLUMN_QUALIFIER));
      outCollector.collect(outKey,
              new Put(Bytes.toBytes("rowKey2"))
              .addColumn(COLUMN_FAMILY, COLUMN_QUALIFIER, Bytes.toBytes(name)));
    }
  }
}
