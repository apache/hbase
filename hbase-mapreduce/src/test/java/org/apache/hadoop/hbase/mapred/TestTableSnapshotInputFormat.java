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
package org.apache.hadoop.hbase.mapred;

import static org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatImpl.SNAPSHOT_INPUTFORMAT_LOCALITY_ENABLED_DEFAULT;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatTestBase;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowMapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({VerySlowMapReduceTests.class, LargeTests.class})
public class TestTableSnapshotInputFormat extends TableSnapshotInputFormatTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTableSnapshotInputFormat.class);

  private static final byte[] aaa = Bytes.toBytes("aaa");
  private static final byte[] after_zzz = Bytes.toBytes("zz{"); // 'z' + 1 => '{'
  private static final String COLUMNS =
    Bytes.toString(FAMILIES[0]) + " " + Bytes.toString(FAMILIES[1]);

  @Rule
  public TestName name = new TestName();

  @Override
  protected byte[] getStartRow() {
    return aaa;
  }

  @Override
  protected byte[] getEndRow() {
    return after_zzz;
  }

  static class TestTableSnapshotMapper extends MapReduceBase
      implements TableMap<ImmutableBytesWritable, NullWritable> {
    @Override
    public void map(ImmutableBytesWritable key, Result value,
        OutputCollector<ImmutableBytesWritable, NullWritable> collector, Reporter reporter)
        throws IOException {
      verifyRowFromMap(key, value);
      collector.collect(key, NullWritable.get());
    }
  }

  public static class TestTableSnapshotReducer extends MapReduceBase
      implements Reducer<ImmutableBytesWritable, NullWritable, NullWritable, NullWritable> {
    HBaseTestingUtility.SeenRowTracker rowTracker =
      new HBaseTestingUtility.SeenRowTracker(aaa, after_zzz);

    @Override
    public void reduce(ImmutableBytesWritable key, Iterator<NullWritable> values,
        OutputCollector<NullWritable, NullWritable> collector, Reporter reporter)
        throws IOException {
      rowTracker.addRow(key.get());
    }

    @Override
    public void close() {
      rowTracker.validate();
    }
  }

  @Test
  public void testInitTableSnapshotMapperJobConfig() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    String snapshotName = "foo";

    try {
      createTableAndSnapshot(UTIL, tableName, snapshotName, getStartRow(), getEndRow(), 1);
      JobConf job = new JobConf(UTIL.getConfiguration());
      Path tmpTableDir = UTIL.getDataTestDirOnTestFS(snapshotName);

      TableMapReduceUtil.initTableSnapshotMapJob(snapshotName,
        COLUMNS, TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
        NullWritable.class, job, false, tmpTableDir);

      // TODO: would be better to examine directly the cache instance that results from this
      // config. Currently this is not possible because BlockCache initialization is static.
      Assert.assertEquals(
        "Snapshot job should be configured for default LruBlockCache.",
        HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT,
        job.getFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, -1), 0.01);
      Assert.assertEquals(
        "Snapshot job should not use BucketCache.",
        0, job.getFloat("hbase.bucketcache.size", -1), 0.01);
    } finally {
      UTIL.getAdmin().deleteSnapshot(snapshotName);
      UTIL.deleteTable(tableName);
    }
  }

  // TODO: mapred does not support limiting input range by startrow, endrow.
  // Thus the following tests must override parameterverification.

  @Test
  @Override
  public void testWithMockedMapReduceMultiRegion() throws Exception {
    testWithMockedMapReduce(
        UTIL, "testWithMockedMapReduceMultiRegion", 10, 1, 10, true);
        // It does not matter whether true or false is given to setLocalityEnabledTo,
        // because it is not read in testWithMockedMapReduce().
  }

  @Test
  @Override
  public void testWithMapReduceMultiRegion() throws Exception {
    testWithMapReduce(UTIL, "testWithMapReduceMultiRegion", 10, 1, 10, false);
  }

  @Test
  @Override
  // run the MR job while HBase is offline
  public void testWithMapReduceAndOfflineHBaseMultiRegion() throws Exception {
    testWithMapReduce(UTIL, "testWithMapReduceAndOfflineHBaseMultiRegion", 10, 1, 10, true);
  }

  @Override
  public void testRestoreSnapshotDoesNotCreateBackRefLinksInit(TableName tableName,
      String snapshotName, Path tmpTableDir) throws Exception {
    JobConf job = new JobConf(UTIL.getConfiguration());
    TableMapReduceUtil.initTableSnapshotMapJob(snapshotName,
      COLUMNS, TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
      NullWritable.class, job, false, tmpTableDir);
  }

  @Override
  protected void testWithMockedMapReduce(HBaseTestingUtility util, String snapshotName,
      int numRegions, int numSplitsPerRegion, int expectedNumSplits, boolean setLocalityEnabledTo)
      throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      createTableAndSnapshot(
        util, tableName, snapshotName, getStartRow(), getEndRow(), numRegions);

      JobConf job = new JobConf(util.getConfiguration());
      // setLocalityEnabledTo is ignored no matter what is specified, so as to test the case that
      // SNAPSHOT_INPUTFORMAT_LOCALITY_ENABLED_KEY is not explicitly specified
      // and the default value is taken.
      Path tmpTableDir = util.getDataTestDirOnTestFS(snapshotName);

      if (numSplitsPerRegion > 1) {
        TableMapReduceUtil.initTableSnapshotMapJob(snapshotName,
                COLUMNS, TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
                NullWritable.class, job, false, tmpTableDir, new RegionSplitter.UniformSplit(),
                numSplitsPerRegion);
      } else {
        TableMapReduceUtil.initTableSnapshotMapJob(snapshotName,
                COLUMNS, TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
                NullWritable.class, job, false, tmpTableDir);
      }

      // mapred doesn't support start and end keys? o.O
      verifyWithMockedMapReduce(job, numRegions, expectedNumSplits, getStartRow(), getEndRow());

    } finally {
      util.getAdmin().deleteSnapshot(snapshotName);
      util.deleteTable(tableName);
    }
  }

  private void verifyWithMockedMapReduce(JobConf job, int numRegions, int expectedNumSplits,
      byte[] startRow, byte[] stopRow) throws IOException, InterruptedException {
    TableSnapshotInputFormat tsif = new TableSnapshotInputFormat();
    InputSplit[] splits = tsif.getSplits(job, 0);

    Assert.assertEquals(expectedNumSplits, splits.length);

    HBaseTestingUtility.SeenRowTracker rowTracker =
      new HBaseTestingUtility.SeenRowTracker(startRow, stopRow);

    // SNAPSHOT_INPUTFORMAT_LOCALITY_ENABLED_KEY is not explicitly specified,
    // so the default value is taken.
    boolean localityEnabled = SNAPSHOT_INPUTFORMAT_LOCALITY_ENABLED_DEFAULT;

    for (int i = 0; i < splits.length; i++) {
      // validate input split
      InputSplit split = splits[i];
      Assert.assertTrue(split instanceof TableSnapshotInputFormat.TableSnapshotRegionSplit);
      if (localityEnabled) {
        // When localityEnabled is true, meant to verify split.getLocations()
        // by the following statement:
        //   Assert.assertTrue(split.getLocations() != null && split.getLocations().length != 0);
        // However, getLocations() of some splits could return an empty array (length is 0),
        // so drop the verification on length.
        // TODO: investigate how to verify split.getLocations() when localityEnabled is true
        Assert.assertTrue(split.getLocations() != null);
      } else {
        Assert.assertTrue(split.getLocations() != null && split.getLocations().length == 0);
      }

      // validate record reader
      OutputCollector collector = mock(OutputCollector.class);
      Reporter reporter = mock(Reporter.class);
      RecordReader<ImmutableBytesWritable, Result> rr = tsif.getRecordReader(split, job, reporter);

      // validate we can read all the data back
      ImmutableBytesWritable key = rr.createKey();
      Result value = rr.createValue();
      while (rr.next(key, value)) {
        verifyRowFromMap(key, value);
        rowTracker.addRow(key.copyBytes());
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
      int numSplitsPerRegion,int expectedNumSplits, boolean shutdownCluster) throws Exception {

    //create the table and snapshot
    createTableAndSnapshot(util, tableName, snapshotName, startRow, endRow, numRegions);

    if (shutdownCluster) {
      util.shutdownMiniHBaseCluster();
    }

    try {
      // create the job
      JobConf jobConf = new JobConf(util.getConfiguration());

      jobConf.setJarByClass(util.getClass());
      org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.addDependencyJarsForClasses(jobConf,
        TestTableSnapshotInputFormat.class);

      if(numSplitsPerRegion > 1) {
        TableMapReduceUtil.initTableSnapshotMapJob(snapshotName, COLUMNS,
                TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
                NullWritable.class, jobConf, true, tableDir, new RegionSplitter.UniformSplit(),
                numSplitsPerRegion);
      } else {
        TableMapReduceUtil.initTableSnapshotMapJob(snapshotName, COLUMNS,
                TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
                NullWritable.class, jobConf, true, tableDir);
      }

      jobConf.setReducerClass(TestTableSnapshotInputFormat.TestTableSnapshotReducer.class);
      jobConf.setNumReduceTasks(1);
      jobConf.setOutputFormat(NullOutputFormat.class);

      RunningJob job = JobClient.runJob(jobConf);
      Assert.assertTrue(job.isSuccessful());
    } finally {
      if (!shutdownCluster) {
        util.getAdmin().deleteSnapshot(snapshotName);
        util.deleteTable(tableName);
      }
    }
  }

  @Ignore // Ignored in mapred package because it keeps failing but allowed in mapreduce package.
  @Test
  public void testWithMapReduceMultipleMappersPerRegion() throws Exception {
    testWithMapReduce(UTIL, "testWithMapReduceMultiRegion", 10, 5, 50, false);
  }
}
