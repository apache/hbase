/*
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
package org.apache.hadoop.hbase.backup.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.util.BulkLoadProcessor;
import org.apache.hadoop.hbase.mapreduce.WALInputFormat;
import org.apache.hadoop.hbase.mapreduce.WALPlayer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Unit tests for BulkLoadCollectorJob (mapper, reducer and job creation/validation).
 */
@Category({ MapReduceTests.class, LargeTests.class })
public class TestBulkLoadCollectorJob {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBulkLoadCollectorJob.class);

  private Configuration conf;

  @Before
  public void setUp() {
    // fresh configuration for each test
    conf = HBaseConfiguration.create();
  }

  @After
  public void tearDown() {
    // nothing for now
  }

  /**
   * Ensures {@link BulkLoadCollectorJob#createSubmittableJob(String[])} correctly configures
   * input/output paths and parses time options into the job configuration.
   */
  @Test
  public void testCreateSubmittableJobValid() throws Exception {
    // set a start time option to make sure setupTime runs and applies it
    String dateStr = "2001-02-20T16:35:06.99";
    conf.set(WALInputFormat.START_TIME_KEY, dateStr);

    BulkLoadCollectorJob jobDriver = new BulkLoadCollectorJob(conf);
    String inputDirs = new Path("file:/wals/input").toString();
    String outDir = new Path("file:/out/bulk").toString();
    Job job = jobDriver.createSubmittableJob(new String[] { inputDirs, outDir });

    // Input path set
    Path[] inPaths = FileInputFormat.getInputPaths(job);
    assertEquals(1, inPaths.length);
    assertEquals(inputDirs, inPaths[0].toString());

    // Output path set
    Path out = FileOutputFormat.getOutputPath(job);
    assertEquals(new Path(outDir), out);

    // Ensure the conf had START_TIME_KEY parsed to a long (setupTime executed)
    long parsed = conf.getLong(WALInputFormat.START_TIME_KEY, -1L);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS");
    long expected = sdf.parse(dateStr).getTime();
    assertEquals(expected, parsed);
  }

  /**
   * Verifies that {@link BulkLoadCollectorJob#createSubmittableJob(String[])} throws an IOException
   * when called with insufficient or null arguments.
   */
  @Test(expected = IOException.class)
  public void testCreateSubmittableJob_throwsForInsufficientArgs() throws Exception {
    BulkLoadCollectorJob jobDriver = new BulkLoadCollectorJob(conf);
    // this call must throw IOException for the test to pass
    jobDriver.createSubmittableJob(new String[] { "file:/only/one/arg" });
  }

  @Test(expected = IOException.class)
  public void testCreateSubmittableJob_throwsForNullArgs() throws Exception {
    BulkLoadCollectorJob jobDriver = new BulkLoadCollectorJob(conf);
    // this call must throw IOException for the test to pass
    jobDriver.createSubmittableJob(null);
  }

  /**
   * Verifies that {@link BulkLoadCollectorJob.BulkLoadCollectorMapper} ignores WAL entries whose
   * table is not present in the configured tables map.
   */
  @Test
  public void testMapperIgnoresWhenTableNotInMap() throws Exception {
    // Prepare mapper and a mocked MapReduce context
    BulkLoadCollectorJob.BulkLoadCollectorMapper mapper =
      new BulkLoadCollectorJob.BulkLoadCollectorMapper();
    @SuppressWarnings("unchecked")
    Mapper<WALKey, WALEdit, Text, NullWritable>.Context ctx = mock(Mapper.Context.class);

    // Build a Configuration that only allows a single table: ns:allowed
    // Note: TABLES_KEY / TABLE_MAP_KEY are the same constants used by the mapper.setup(...)
    Configuration cfgForTest = new Configuration(conf);
    cfgForTest.setStrings(WALPlayer.TABLES_KEY, "ns:allowed");
    cfgForTest.setStrings(WALPlayer.TABLE_MAP_KEY, "ns:allowed"); // maps to itself

    // Have the mocked context return our test configuration when mapper.setup() runs
    when(ctx.getConfiguration()).thenReturn(cfgForTest);
    mapper.setup(ctx);

    // Create a WALKey for a table that is NOT in the allowed map (ns:other)
    WALKey keyForOtherTable = mock(WALKey.class);
    when(keyForOtherTable.getTableName()).thenReturn(TableName.valueOf("ns:other"));
    WALEdit walEdit = mock(WALEdit.class);

    // Static-mock BulkLoadProcessor to ensure it would not be relied on:
    // even if invoked unexpectedly, it returns a non-empty list, but we will assert no writes
    // occurred.
    try (MockedStatic<BulkLoadProcessor> proc = Mockito.mockStatic(BulkLoadProcessor.class)) {
      proc.when(() -> BulkLoadProcessor.processBulkLoadFiles(any(), any()))
        .thenReturn(Collections.singletonList(new Path("x")));

      // Invoke mapper - because the table is not allowed, mapper should do nothing
      mapper.map(keyForOtherTable, walEdit, ctx);

      // Assert: mapper did not write any output to the context
      verify(ctx, never()).write(any(Text.class), any(NullWritable.class));
    }
  }

  /**
   * Verifies that {@link BulkLoadCollectorJob.BulkLoadCollectorMapper} safely handles null inputs.
   * <p>
   * The mapper should ignore WAL entries when either the WAL key or the WALEdit value is null, and
   * must not emit any output in those cases.
   * </p>
   * @throws Exception on test failure
   */
  @Test
  public void testMapperHandlesNullKeyOrValue() throws Exception {
    BulkLoadCollectorJob.BulkLoadCollectorMapper mapper =
      new BulkLoadCollectorJob.BulkLoadCollectorMapper();
    @SuppressWarnings("unchecked")
    Mapper<WALKey, WALEdit, Text, NullWritable>.Context ctx = mock(Mapper.Context.class);
    when(ctx.getConfiguration()).thenReturn(conf);
    mapper.setup(ctx);

    // null key
    mapper.map(null, mock(WALEdit.class), ctx);
    // null value
    mapper.map(mock(WALKey.class), null, ctx);

    // ensure no writes
    verify(ctx, never()).write(any(Text.class), any(NullWritable.class));
  }

  /**
   * Verifies that {@link BulkLoadCollectorJob.DedupReducer} writes each unique key exactly once.
   */
  @Test
  public void testDedupReducerWritesOnce() throws Exception {
    BulkLoadCollectorJob.DedupReducer reducer = new BulkLoadCollectorJob.DedupReducer();
    @SuppressWarnings("unchecked")
    Reducer<Text, NullWritable, Text, NullWritable>.Context ctx = mock(Reducer.Context.class);

    Text key = new Text("/some/path");

    // Simulate three duplicate values for the same key; reducer should still write the key once.
    Iterable<NullWritable> vals =
      Arrays.asList(NullWritable.get(), NullWritable.get(), NullWritable.get());

    reducer.reduce(key, vals, ctx);

    // verify exactly once write with the same key
    verify(ctx, times(1)).write(eq(key), eq(NullWritable.get()));
  }
}
