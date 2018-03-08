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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.mapreduce.WALInputFormat.WALKeyRecordReader;
import org.apache.hadoop.hbase.mapreduce.WALInputFormat.WALRecordReader;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit tests for the WALRecordReader
 */
@Category({ MapReduceTests.class, MediumTests.class })
public class TestWALRecordReader {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWALRecordReader.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestWALRecordReader.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;
  private static FileSystem fs;
  private static Path hbaseDir;
  private static FileSystem walFs;
  private static Path walRootDir;
  // visible for TestHLogRecordReader
  static final TableName tableName = TableName.valueOf(getName());
  private static final byte [] rowName = tableName.getName();
  // visible for TestHLogRecordReader
  static final RegionInfo info = RegionInfoBuilder.newBuilder(tableName).build();
  private static final byte[] family = Bytes.toBytes("column");
  private static final byte[] value = Bytes.toBytes("value");
  private static Path logDir;
  protected MultiVersionConcurrencyControl mvcc;
  protected static NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  private static String getName() {
    return "TestWALRecordReader";
  }

  @Before
  public void setUp() throws Exception {
    fs.delete(hbaseDir, true);
    walFs.delete(walRootDir, true);
    mvcc = new MultiVersionConcurrencyControl();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Make block sizes small.
    conf = TEST_UTIL.getConfiguration();
    conf.setInt("dfs.blocksize", 1024 * 1024);
    conf.setInt("dfs.replication", 1);
    TEST_UTIL.startMiniDFSCluster(1);

    conf = TEST_UTIL.getConfiguration();
    fs = TEST_UTIL.getDFSCluster().getFileSystem();

    hbaseDir = TEST_UTIL.createRootDir();
    walRootDir = TEST_UTIL.createWALRootDir();
    walFs = FSUtils.getWALFileSystem(conf);
    logDir = new Path(walRootDir, HConstants.HREGION_LOGDIR_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    fs.delete(hbaseDir, true);
    walFs.delete(walRootDir, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test partial reads from the log based on passed time range
   * @throws Exception
   */
  @Test
  public void testPartialRead() throws Exception {
    final WALFactory walfactory = new WALFactory(conf, getName());
    WAL log = walfactory.getWAL(info);
    // This test depends on timestamp being millisecond based and the filename of the WAL also
    // being millisecond based.
    long ts = System.currentTimeMillis();
    WALEdit edit = new WALEdit();
    edit.add(new KeyValue(rowName, family, Bytes.toBytes("1"), ts, value));
    log.append(info, getWalKeyImpl(ts, scopes), edit, true);
    edit = new WALEdit();
    edit.add(new KeyValue(rowName, family, Bytes.toBytes("2"), ts+1, value));
    log.append(info, getWalKeyImpl(ts+1, scopes), edit, true);
    log.sync();
    LOG.info("Before 1st WAL roll " + log.toString());
    log.rollWriter();
    LOG.info("Past 1st WAL roll " + log.toString());

    Thread.sleep(1);
    long ts1 = System.currentTimeMillis();

    edit = new WALEdit();
    edit.add(new KeyValue(rowName, family, Bytes.toBytes("3"), ts1+1, value));
    log.append(info, getWalKeyImpl(ts1+1, scopes), edit, true);
    edit = new WALEdit();
    edit.add(new KeyValue(rowName, family, Bytes.toBytes("4"), ts1+2, value));
    log.append(info, getWalKeyImpl(ts1+2, scopes), edit, true);
    log.sync();
    log.shutdown();
    walfactory.shutdown();
    LOG.info("Closed WAL " + log.toString());


    WALInputFormat input = new WALInputFormat();
    Configuration jobConf = new Configuration(conf);
    jobConf.set("mapreduce.input.fileinputformat.inputdir", logDir.toString());
    jobConf.setLong(WALInputFormat.END_TIME_KEY, ts);

    // only 1st file is considered, and only its 1st entry is used
    List<InputSplit> splits = input.getSplits(MapreduceTestingShim.createJobContext(jobConf));

    assertEquals(1, splits.size());
    testSplit(splits.get(0), Bytes.toBytes("1"));

    jobConf.setLong(WALInputFormat.START_TIME_KEY, ts+1);
    jobConf.setLong(WALInputFormat.END_TIME_KEY, ts1+1);
    splits = input.getSplits(MapreduceTestingShim.createJobContext(jobConf));
    // both files need to be considered
    assertEquals(2, splits.size());
    // only the 2nd entry from the 1st file is used
    testSplit(splits.get(0), Bytes.toBytes("2"));
    // only the 1nd entry from the 2nd file is used
    testSplit(splits.get(1), Bytes.toBytes("3"));
  }

  /**
   * Test basic functionality
   * @throws Exception
   */
  @Test
  public void testWALRecordReader() throws Exception {
    final WALFactory walfactory = new WALFactory(conf, getName());
    WAL log = walfactory.getWAL(info);
    byte [] value = Bytes.toBytes("value");
    WALEdit edit = new WALEdit();
    edit.add(new KeyValue(rowName, family, Bytes.toBytes("1"),
        System.currentTimeMillis(), value));
    long txid = log.append(info, getWalKeyImpl(System.currentTimeMillis(), scopes), edit, true);
    log.sync(txid);

    Thread.sleep(1); // make sure 2nd log gets a later timestamp
    long secondTs = System.currentTimeMillis();
    log.rollWriter();

    edit = new WALEdit();
    edit.add(new KeyValue(rowName, family, Bytes.toBytes("2"),
        System.currentTimeMillis(), value));
    txid = log.append(info, getWalKeyImpl(System.currentTimeMillis(), scopes), edit, true);
    log.sync(txid);
    log.shutdown();
    walfactory.shutdown();
    long thirdTs = System.currentTimeMillis();

    // should have 2 log files now
    WALInputFormat input = new WALInputFormat();
    Configuration jobConf = new Configuration(conf);
    jobConf.set("mapreduce.input.fileinputformat.inputdir", logDir.toString());

    // make sure both logs are found
    List<InputSplit> splits = input.getSplits(MapreduceTestingShim.createJobContext(jobConf));
    assertEquals(2, splits.size());

    // should return exactly one KV
    testSplit(splits.get(0), Bytes.toBytes("1"));
    // same for the 2nd split
    testSplit(splits.get(1), Bytes.toBytes("2"));

    // now test basic time ranges:

    // set an endtime, the 2nd log file can be ignored completely.
    jobConf.setLong(WALInputFormat.END_TIME_KEY, secondTs-1);
    splits = input.getSplits(MapreduceTestingShim.createJobContext(jobConf));
    assertEquals(1, splits.size());
    testSplit(splits.get(0), Bytes.toBytes("1"));

    // now set a start time
    jobConf.setLong(WALInputFormat.END_TIME_KEY, Long.MAX_VALUE);
    jobConf.setLong(WALInputFormat.START_TIME_KEY, thirdTs);
    splits = input.getSplits(MapreduceTestingShim.createJobContext(jobConf));
    // both logs need to be considered
    assertEquals(2, splits.size());
    // but both readers skip all edits
    testSplit(splits.get(0));
    testSplit(splits.get(1));
  }

  /**
   * Test WALRecordReader tolerance to moving WAL from active
   * to archive directory
   * @throws Exception exception
   */
  @Test
  public void testWALRecordReaderActiveArchiveTolerance() throws Exception {
    final WALFactory walfactory = new WALFactory(conf, getName());
    WAL log = walfactory.getWAL(info);
    byte [] value = Bytes.toBytes("value");
    WALEdit edit = new WALEdit();
    edit.add(new KeyValue(rowName, family, Bytes.toBytes("1"),
        System.currentTimeMillis(), value));
    long txid = log.append(info, getWalKeyImpl(System.currentTimeMillis(), scopes), edit, true);
    log.sync(txid);

    Thread.sleep(10); // make sure 2nd edit gets a later timestamp

    edit = new WALEdit();
    edit.add(new KeyValue(rowName, family, Bytes.toBytes("2"),
        System.currentTimeMillis(), value));
    txid = log.append(info, getWalKeyImpl(System.currentTimeMillis(), scopes), edit, true);
    log.sync(txid);
    log.shutdown();

    // should have 2 log entries now
    WALInputFormat input = new WALInputFormat();
    Configuration jobConf = new Configuration(conf);
    jobConf.set("mapreduce.input.fileinputformat.inputdir", logDir.toString());
    // make sure log is found
    List<InputSplit> splits = input.getSplits(MapreduceTestingShim.createJobContext(jobConf));
    assertEquals(1, splits.size());
    WALInputFormat.WALSplit split = (WALInputFormat.WALSplit) splits.get(0);
    LOG.debug("log="+logDir+" file="+ split.getLogFileName());

    testSplitWithMovingWAL(splits.get(0), Bytes.toBytes("1"), Bytes.toBytes("2"));

  }

  protected WALKeyImpl getWalKeyImpl(final long time, NavigableMap<byte[], Integer> scopes) {
    return new WALKeyImpl(info.getEncodedNameAsBytes(), tableName, time, mvcc, scopes);
  }

  private WALRecordReader<WALKey> getReader() {
    return new WALKeyRecordReader();
  }

  /**
   * Create a new reader from the split, and match the edits against the passed columns.
   */
  private void testSplit(InputSplit split, byte[]... columns) throws Exception {
    WALRecordReader<WALKey> reader = getReader();
    reader.initialize(split, MapReduceTestUtil.createDummyMapTaskAttemptContext(conf));

    for (byte[] column : columns) {
      assertTrue(reader.nextKeyValue());
      Cell cell = reader.getCurrentValue().getCells().get(0);
      if (!Bytes.equals(column, 0, column.length, cell.getQualifierArray(),
        cell.getQualifierOffset(), cell.getQualifierLength())) {
        assertTrue(
          "expected [" + Bytes.toString(column) + "], actual [" + Bytes.toString(
            cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "]",
          false);
      }
    }
    assertFalse(reader.nextKeyValue());
    reader.close();
  }

  /**
   * Create a new reader from the split, match the edits against the passed columns,
   * moving WAL to archive in between readings
   */
  private void testSplitWithMovingWAL(InputSplit split, byte[] col1, byte[] col2) throws Exception {
    WALRecordReader<WALKey> reader = getReader();
    reader.initialize(split, MapReduceTestUtil.createDummyMapTaskAttemptContext(conf));

    assertTrue(reader.nextKeyValue());
    Cell cell = reader.getCurrentValue().getCells().get(0);
    if (!Bytes.equals(col1, 0, col1.length, cell.getQualifierArray(), cell.getQualifierOffset(),
      cell.getQualifierLength())) {
      assertTrue(
        "expected [" + Bytes.toString(col1) + "], actual [" + Bytes.toString(
          cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "]",
        false);
    }
    // Move log file to archive directory
    // While WAL record reader is open
    WALInputFormat.WALSplit split_ = (WALInputFormat.WALSplit) split;

    Path logFile = new Path(split_.getLogFileName());
    Path archivedLog = AbstractFSWALProvider.getArchivedLogPath(logFile, conf);
    boolean result = fs.rename(logFile, archivedLog);
    assertTrue(result);
    result = fs.exists(archivedLog);
    assertTrue(result);
    assertTrue(reader.nextKeyValue());
    cell = reader.getCurrentValue().getCells().get(0);
    if (!Bytes.equals(col2, 0, col2.length, cell.getQualifierArray(), cell.getQualifierOffset(),
      cell.getQualifierLength())) {
      assertTrue(
        "expected [" + Bytes.toString(col2) + "], actual [" + Bytes.toString(
          cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "]",
        false);
    }
    reader.close();
  }
}