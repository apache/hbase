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

import static org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatImpl.SNAPSHOT_INPUTFORMAT_LOCALITY_ENABLED_DEFAULT;
import static org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatImpl.SNAPSHOT_INPUTFORMAT_LOCALITY_ENABLED_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TestTableSnapshotScanner;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat.TableSnapshotRegionSplit;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowMapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category({VerySlowMapReduceTests.class, LargeTests.class})
public class TestTableSnapshotInputFormat extends TableSnapshotInputFormatTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTableSnapshotInputFormat.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestTableSnapshotInputFormat.class);

  private static final byte[] bbb = Bytes.toBytes("bbb");
  private static final byte[] yyy = Bytes.toBytes("yyy");
  private static final byte[] bbc = Bytes.toBytes("bbc");
  private static final byte[] yya = Bytes.toBytes("yya");

  @Rule
  public TestName name = new TestName();

  @Override
  protected byte[] getStartRow() {
    return bbb;
  }

  @Override
  protected byte[] getEndRow() {
    return yyy;
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testGetBestLocations() throws IOException {
    TableSnapshotInputFormatImpl tsif = new TableSnapshotInputFormatImpl();
    Configuration conf = UTIL.getConfiguration();

    HDFSBlocksDistribution blockDistribution = new HDFSBlocksDistribution();
    Assert.assertEquals(null,
      TableSnapshotInputFormatImpl.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h1"}, 1);
    Assert.assertEquals(Lists.newArrayList("h1"),
      TableSnapshotInputFormatImpl.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h1"}, 1);
    Assert.assertEquals(Lists.newArrayList("h1"),
      TableSnapshotInputFormatImpl.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h2"}, 1);
    Assert.assertEquals(Lists.newArrayList("h1"),
      TableSnapshotInputFormatImpl.getBestLocations(conf, blockDistribution));

    blockDistribution = new HDFSBlocksDistribution();
    blockDistribution.addHostsAndBlockWeight(new String[] {"h1"}, 10);
    blockDistribution.addHostsAndBlockWeight(new String[] {"h2"}, 7);
    blockDistribution.addHostsAndBlockWeight(new String[] {"h3"}, 5);
    blockDistribution.addHostsAndBlockWeight(new String[] {"h4"}, 1);
    Assert.assertEquals(Lists.newArrayList("h1"),
      TableSnapshotInputFormatImpl.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h2"}, 2);
    Assert.assertEquals(Lists.newArrayList("h1", "h2"),
      TableSnapshotInputFormatImpl.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h2"}, 3);
    Assert.assertEquals(Lists.newArrayList("h2", "h1"),
      TableSnapshotInputFormatImpl.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h3"}, 6);
    blockDistribution.addHostsAndBlockWeight(new String[] {"h4"}, 9);

    Assert.assertEquals(Lists.newArrayList("h2", "h3", "h4"),
      TableSnapshotInputFormatImpl.getBestLocations(conf, blockDistribution));
  }

  public static enum TestTableSnapshotCounters {
    VALIDATION_ERROR
  }

  public static class TestTableSnapshotMapper
    extends TableMapper<ImmutableBytesWritable, NullWritable> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value,
        Context context) throws IOException, InterruptedException {
      // Validate a single row coming from the snapshot, and emit the row key
      verifyRowFromMap(key, value);
      context.write(key, NullWritable.get());
    }
  }

  public static class TestTableSnapshotReducer
    extends Reducer<ImmutableBytesWritable, NullWritable, NullWritable, NullWritable> {
    HBaseTestingUtility.SeenRowTracker rowTracker =
        new HBaseTestingUtility.SeenRowTracker(bbb, yyy);
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<NullWritable> values,
       Context context) throws IOException, InterruptedException {
      rowTracker.addRow(key.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      rowTracker.validate();
    }
  }

  @Test
  public void testInitTableSnapshotMapperJobConfig() throws Exception {
    setupCluster();
    final TableName tableName = TableName.valueOf(name.getMethodName());
    String snapshotName = "foo";

    try {
      createTableAndSnapshot(UTIL, tableName, snapshotName, getStartRow(), getEndRow(), 1);
      Job job = new Job(UTIL.getConfiguration());
      Path tmpTableDir = UTIL.getDataTestDirOnTestFS(snapshotName);

      TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName,
        new Scan(), TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
        NullWritable.class, job, false, tmpTableDir);

      // TODO: would be better to examine directly the cache instance that results from this
      // config. Currently this is not possible because BlockCache initialization is static.
      Assert.assertEquals(
        "Snapshot job should be configured for default LruBlockCache.",
        HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT,
        job.getConfiguration().getFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, -1), 0.01);
      Assert.assertEquals(
        "Snapshot job should not use BucketCache.",
        0, job.getConfiguration().getFloat("hbase.bucketcache.size", -1), 0.01);
    } finally {
      UTIL.getAdmin().deleteSnapshot(snapshotName);
      UTIL.deleteTable(tableName);
      tearDownCluster();
    }
  }

  @Override
  public void testRestoreSnapshotDoesNotCreateBackRefLinksInit(TableName tableName,
      String snapshotName, Path tmpTableDir) throws Exception {
    Job job = new Job(UTIL.getConfiguration());
    TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName,
      new Scan(), TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
      NullWritable.class, job, false, tmpTableDir);
  }

  @Override
  public void testWithMockedMapReduce(HBaseTestingUtility util, String snapshotName,
      int numRegions, int numSplitsPerRegion, int expectedNumSplits, boolean setLocalityEnabledTo)
      throws Exception {
    setupCluster();
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      createTableAndSnapshot(
        util, tableName, snapshotName, getStartRow(), getEndRow(), numRegions);

      Configuration conf = util.getConfiguration();
      conf.setBoolean(SNAPSHOT_INPUTFORMAT_LOCALITY_ENABLED_KEY, setLocalityEnabledTo);
      Job job = new Job(conf);
      Path tmpTableDir = util.getDataTestDirOnTestFS(snapshotName);
      Scan scan = new Scan().withStartRow(getStartRow()).withStopRow(getEndRow()); // limit the scan

      if (numSplitsPerRegion > 1) {
        TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName,
                scan, TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
                NullWritable.class, job, false, tmpTableDir, new RegionSplitter.UniformSplit(),
                numSplitsPerRegion);
      } else {
        TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName,
                scan, TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
                NullWritable.class, job, false, tmpTableDir);
      }

      verifyWithMockedMapReduce(job, numRegions, expectedNumSplits, getStartRow(), getEndRow());

    } finally {
      util.getAdmin().deleteSnapshot(snapshotName);
      util.deleteTable(tableName);
      tearDownCluster();
    }
  }

  @Test
  public void testWithMockedMapReduceWithSplitsPerRegion() throws Exception {
    setupCluster();
    String snapshotName = "testWithMockedMapReduceMultiRegion";
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      createTableAndSnapshot(UTIL, tableName, snapshotName, getStartRow(), getEndRow(), 10);

      Configuration conf = UTIL.getConfiguration();
      conf.setBoolean(SNAPSHOT_INPUTFORMAT_LOCALITY_ENABLED_KEY, false);
      Job job = new Job(conf);
      Path tmpTableDir = UTIL.getDataTestDirOnTestFS(snapshotName);
      // test scan with startRow and stopRow
      Scan scan = new Scan().withStartRow(bbc).withStopRow(yya);

      TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName, scan,
        TestTableSnapshotMapper.class, ImmutableBytesWritable.class, NullWritable.class, job, false,
        tmpTableDir, new RegionSplitter.UniformSplit(), 5);

      verifyWithMockedMapReduce(job, 10, 40, bbc, yya);
    } finally {
      UTIL.getAdmin().deleteSnapshot(snapshotName);
      UTIL.deleteTable(tableName);
      tearDownCluster();
    }
  }

  @Test
  public void testWithMockedMapReduceWithNoStartRowStopRow() throws Exception {
    setupCluster();
    String snapshotName = "testWithMockedMapReduceMultiRegion";
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      createTableAndSnapshot(UTIL, tableName, snapshotName, getStartRow(), getEndRow(), 10);

      Configuration conf = UTIL.getConfiguration();
      conf.setBoolean(SNAPSHOT_INPUTFORMAT_LOCALITY_ENABLED_KEY, false);
      Job job = new Job(conf);
      Path tmpTableDir = UTIL.getDataTestDirOnTestFS(snapshotName);
      // test scan without startRow and stopRow
      Scan scan2 = new Scan();

      TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName, scan2,
        TestTableSnapshotMapper.class, ImmutableBytesWritable.class, NullWritable.class, job, false,
        tmpTableDir, new RegionSplitter.UniformSplit(), 5);

      verifyWithMockedMapReduce(job, 10, 50, HConstants.EMPTY_START_ROW,
        HConstants.EMPTY_START_ROW);

    } finally {
      UTIL.getAdmin().deleteSnapshot(snapshotName);
      UTIL.deleteTable(tableName);
      tearDownCluster();
    }
  }

  @Test
  public void testNoDuplicateResultsWhenSplitting() throws Exception {
    setupCluster();
    TableName tableName = TableName.valueOf("testNoDuplicateResultsWhenSplitting");
    String snapshotName = "testSnapshotBug";
    try {
      if (UTIL.getAdmin().tableExists(tableName)) {
        UTIL.deleteTable(tableName);
      }

      UTIL.createTable(tableName, FAMILIES);
      Admin admin = UTIL.getAdmin();

      // put some stuff in the table
      Table table = UTIL.getConnection().getTable(tableName);
      UTIL.loadTable(table, FAMILIES);

      // split to 2 regions
      admin.split(tableName, Bytes.toBytes("eee"));
      TestTableSnapshotScanner.blockUntilSplitFinished(UTIL, tableName, 2);

      Path rootDir = FSUtils.getRootDir(UTIL.getConfiguration());
      FileSystem fs = rootDir.getFileSystem(UTIL.getConfiguration());

      SnapshotTestingUtils.createSnapshotAndValidate(admin, tableName, Arrays.asList(FAMILIES),
        null, snapshotName, rootDir, fs, true);

      // load different values
      byte[] value = Bytes.toBytes("after_snapshot_value");
      UTIL.loadTable(table, FAMILIES, value);

      // cause flush to create new files in the region
      admin.flush(tableName);
      table.close();

      Job job = new Job(UTIL.getConfiguration());
      Path tmpTableDir = UTIL.getDataTestDirOnTestFS(snapshotName);
      // limit the scan
      Scan scan = new Scan().withStartRow(getStartRow()).withStopRow(getEndRow());

      TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName, scan,
        TestTableSnapshotMapper.class, ImmutableBytesWritable.class, NullWritable.class, job, false,
        tmpTableDir);

      verifyWithMockedMapReduce(job, 2, 2, getStartRow(), getEndRow());
    } finally {
      UTIL.getAdmin().deleteSnapshot(snapshotName);
      UTIL.deleteTable(tableName);
      tearDownCluster();
    }
  }

  private void verifyWithMockedMapReduce(Job job, int numRegions, int expectedNumSplits,
      byte[] startRow, byte[] stopRow)
      throws IOException, InterruptedException {
    TableSnapshotInputFormat tsif = new TableSnapshotInputFormat();
    List<InputSplit> splits = tsif.getSplits(job);

    Assert.assertEquals(expectedNumSplits, splits.size());

    HBaseTestingUtility.SeenRowTracker rowTracker = new HBaseTestingUtility.SeenRowTracker(startRow,
        stopRow.length > 0 ? stopRow : Bytes.toBytes("\uffff"));

    boolean localityEnabled =
        job.getConfiguration().getBoolean(SNAPSHOT_INPUTFORMAT_LOCALITY_ENABLED_KEY,
                                          SNAPSHOT_INPUTFORMAT_LOCALITY_ENABLED_DEFAULT);

    for (int i = 0; i < splits.size(); i++) {
      // validate input split
      InputSplit split = splits.get(i);
      Assert.assertTrue(split instanceof TableSnapshotRegionSplit);
      TableSnapshotRegionSplit snapshotRegionSplit = (TableSnapshotRegionSplit) split;
      if (localityEnabled) {
        Assert.assertTrue(split.getLocations() != null && split.getLocations().length != 0);
      } else {
        Assert.assertTrue(split.getLocations() != null && split.getLocations().length == 0);
      }

      Scan scan =
          TableMapReduceUtil.convertStringToScan(snapshotRegionSplit.getDelegate().getScan());
      if (startRow.length > 0) {
        Assert.assertTrue(
          Bytes.toStringBinary(startRow) + " should <= " + Bytes.toStringBinary(scan.getStartRow()),
          Bytes.compareTo(startRow, scan.getStartRow()) <= 0);
      }
      if (stopRow.length > 0) {
        Assert.assertTrue(
          Bytes.toStringBinary(stopRow) + " should >= " + Bytes.toStringBinary(scan.getStopRow()),
          Bytes.compareTo(stopRow, scan.getStopRow()) >= 0);
      }
      Assert.assertTrue("startRow should < stopRow",
        Bytes.compareTo(scan.getStartRow(), scan.getStopRow()) < 0);

      // validate record reader
      TaskAttemptContext taskAttemptContext = mock(TaskAttemptContext.class);
      when(taskAttemptContext.getConfiguration()).thenReturn(job.getConfiguration());
      RecordReader<ImmutableBytesWritable, Result> rr =
          tsif.createRecordReader(split, taskAttemptContext);
      rr.initialize(split, taskAttemptContext);

      // validate we can read all the data back
      while (rr.nextKeyValue()) {
        byte[] row = rr.getCurrentKey().get();
        verifyRowFromMap(rr.getCurrentKey(), rr.getCurrentValue());
        rowTracker.addRow(row);
      }

      rr.close();
    }

    // validate all rows are seen
    rowTracker.validate();
  }

  @Override
  protected void testWithMapReduceImpl(HBaseTestingUtility util, TableName tableName,
      String snapshotName, Path tableDir, int numRegions, int numSplitsPerRegion,
      int expectedNumSplits, boolean shutdownCluster) throws Exception {
    doTestWithMapReduce(util, tableName, snapshotName, getStartRow(), getEndRow(), tableDir,
      numRegions, numSplitsPerRegion, expectedNumSplits, shutdownCluster);
  }

  // this is also called by the IntegrationTestTableSnapshotInputFormat
  public static void doTestWithMapReduce(HBaseTestingUtility util, TableName tableName,
      String snapshotName, byte[] startRow, byte[] endRow, Path tableDir, int numRegions,
      int numSplitsPerRegion, int expectedNumSplits, boolean shutdownCluster) throws Exception {

    LOG.info("testing with MapReduce");

    LOG.info("create the table and snapshot");
    createTableAndSnapshot(util, tableName, snapshotName, startRow, endRow, numRegions);

    if (shutdownCluster) {
      LOG.info("shutting down hbase cluster.");
      util.shutdownMiniHBaseCluster();
    }

    try {
      // create the job
      Job job = new Job(util.getConfiguration());
      Scan scan = new Scan().withStartRow(startRow).withStopRow(endRow); // limit the scan

      job.setJarByClass(util.getClass());
      TableMapReduceUtil.addDependencyJarsForClasses(job.getConfiguration(),
              TestTableSnapshotInputFormat.class);

      if (numSplitsPerRegion > 1) {
        TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName,
                scan, TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
                NullWritable.class, job, true, tableDir, new RegionSplitter.UniformSplit(),
                numSplitsPerRegion);
      } else {
        TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName,
                scan, TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
                NullWritable.class, job, true, tableDir);
      }

      job.setReducerClass(TestTableSnapshotInputFormat.TestTableSnapshotReducer.class);
      job.setNumReduceTasks(1);
      job.setOutputFormatClass(NullOutputFormat.class);

      Assert.assertTrue(job.waitForCompletion(true));
    } finally {
      if (!shutdownCluster) {
        util.getAdmin().deleteSnapshot(snapshotName);
        util.deleteTable(tableName);
      }
    }
  }

  @Test
  public void testWithMapReduceMultipleMappersPerRegion() throws Exception {
    testWithMapReduce(UTIL, "testWithMapReduceMultiRegion", 10, 5, 50, false);
  }
}
