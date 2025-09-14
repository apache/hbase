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
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.util.BackupFileSystemManager;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

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

  @Test
  public void testCreateSubmittableJobValid() throws Exception {
    // set a start time option to make sure setupTime runs and applies it
    String dateStr = "2001-02-20T16:35:06.99";
    conf.set(WALInputFormat.START_TIME_KEY, dateStr);

    BulkLoadCollectorJob jobDriver = new BulkLoadCollectorJob(conf);
    String inputDirs = "/wals/input";
    String outDir = "/out/bulk";
    Job job = jobDriver.createSubmittableJob(new String[] { inputDirs, outDir });

    // Verify job configured with expected classes/paths
    assertEquals(BulkLoadCollectorJob.NAME + "_" + job.getConfiguration()
      .getLong("mapreduce.job.id", job.getConfiguration().getLong("mapreduce.job.start.time", 0L)),
      job.getJobName()); // loose check — job name contains NAME_ and timestamp

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

  @Test
  public void testCreateSubmittableJobMissingArgsThrows() {
    BulkLoadCollectorJob jobDriver = new BulkLoadCollectorJob(conf);
    try {
      jobDriver.createSubmittableJob(new String[] { "/only/one/arg" });
      fail("Expected IOException for insufficient args");
    } catch (Exception e) {
      // expected
    }

    try {
      jobDriver.createSubmittableJob(null);
      fail("Expected IOException for null args");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testMapperEmitsResolvedPaths() throws Exception {
    // Prepare mapper and mocked context
    BulkLoadCollectorJob.BulkLoadCollectorMapper mapper =
      new BulkLoadCollectorJob.BulkLoadCollectorMapper();
    @SuppressWarnings("unchecked")
    Mapper<WALKey, WALEdit, Text, NullWritable>.Context ctx = mock(Mapper.Context.class);

    // mock input split to return a WAL input path
    FileSplit fileSplit = mock(FileSplit.class);
    Path walInputPath = new Path("/wals/regionserver/wal-0001");
    when(fileSplit.getPath()).thenReturn(walInputPath);
    when(ctx.getInputSplit()).thenReturn(fileSplit);

    // no table filters => tables map will be empty => all tables allowed
    when(ctx.getConfiguration()).thenReturn(conf);
    mapper.setup(ctx);

    // Prepare WAL key/value mocks
    WALKey key = mock(WALKey.class);
    WALEdit value = mock(WALEdit.class);

    // static mock BulkLoadProcessor.processBulkLoadFiles(...) to return a relative path list
    Path rel = new Path("regionA/abc.hfile");
    try (MockedStatic<BulkLoadProcessor> proc = Mockito.mockStatic(BulkLoadProcessor.class);
      MockedStatic<BackupFileSystemManager> bfm =
        Mockito.mockStatic(BackupFileSystemManager.class)) {

      proc.when(() -> BulkLoadProcessor.processBulkLoadFiles(key, value))
        .thenReturn(Collections.singletonList(rel));

      Path resolved = new Path("/hbase/regionA/abc.hfile");
      bfm.when(() -> BackupFileSystemManager.resolveBulkLoadFullPath(walInputPath, rel))
        .thenReturn(resolved);

      // capture writes to context and assert correct full path string emitted
      doAnswer((Answer<Void>) invocation -> {
        Text t = (Text) invocation.getArgument(0);
        NullWritable n = (NullWritable) invocation.getArgument(1);
        assertEquals(resolved.toString(), t.toString());
        return null;
      }).when(ctx).write(any(Text.class), any(NullWritable.class));

      mapper.map(key, value, ctx);
      // verify counter increment attempted
      verify(ctx, atLeastOnce()).getCounter(eq("BulkCollector"), eq("StoreFilesEmitted"));
    }
  }

  @Test
  public void testMapperIgnoresWhenTableNotInMap() throws Exception {
    BulkLoadCollectorJob.BulkLoadCollectorMapper mapper =
      new BulkLoadCollectorJob.BulkLoadCollectorMapper();
    @SuppressWarnings("unchecked")
    Mapper<WALKey, WALEdit, Text, NullWritable>.Context ctx = mock(Mapper.Context.class);

    // configure tables/tablesMap so mapper.setup() will populate the tables map with a different
    // table
    Configuration c = new Configuration(conf);
    // TABLES_KEY and TABLE_MAP_KEY should be constants on the class; they are used by setup()
    c.setStrings(WALPlayer.TABLES_KEY, "ns:allowed");
    c.setStrings(WALPlayer.TABLE_MAP_KEY, "ns:allowed"); // maps to itself
    when(ctx.getConfiguration()).thenReturn(c);
    mapper.setup(ctx);

    // create a WALKey for a table NOT in the map
    WALKey key = mock(WALKey.class);
    when(key.getTableName()).thenReturn(TableName.valueOf("ns:other"));
    WALEdit value = mock(WALEdit.class);

    // If table is not allowed, BulkLoadProcessor should NOT be called; we can static-mock to fail
    // if called
    try (MockedStatic<BulkLoadProcessor> proc = Mockito.mockStatic(BulkLoadProcessor.class)) {
      // If processBulkLoadFiles is invoked unexpectedly, return empty list (but we will verify no
      // writes)
      proc.when(() -> BulkLoadProcessor.processBulkLoadFiles(any(), any()))
        .thenReturn(Collections.singletonList(new Path("x")));
      // run map; should do nothing (no writes)
      mapper.map(key, value, ctx);
      verify(ctx, never()).write(any(Text.class), any(NullWritable.class));
    }
  }

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

  @Test
  public void testDedupReducerWritesOnce() throws Exception {
    BulkLoadCollectorJob.DedupReducer reducer = new BulkLoadCollectorJob.DedupReducer();
    @SuppressWarnings("unchecked")
    Reducer<Text, NullWritable, Text, NullWritable>.Context ctx = mock(Reducer.Context.class);

    Text key = new Text("/some/path");
    Iterable<NullWritable> vals =
      Arrays.asList(NullWritable.get(), NullWritable.get(), NullWritable.get());

    reducer.reduce(key, vals, ctx);

    // verify exactly once write with the same key
    verify(ctx, times(1)).write(eq(key), eq(NullWritable.get()));
  }
}
