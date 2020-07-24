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
package org.apache.hadoop.hbase.mapreduce;

import static org.apache.hadoop.hbase.regionserver.HStoreFile.BLOOM_FILTER_TYPE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HadoopShims;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PerformanceEvaluation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.TagUtil;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.TestHRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowMapReduceTests;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple test for {@link HFileOutputFormat2}.
 * Sets up and runs a mapreduce job that writes hfile output.
 * Creates a few inner classes to implement splits and an inputformat that
 * emits keys and values like those of {@link PerformanceEvaluation}.
 */
@Category({VerySlowMapReduceTests.class, LargeTests.class})
public class TestCellBasedHFileOutputFormat2  {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCellBasedHFileOutputFormat2.class);

  private final static int ROWSPERSPLIT = 1024;

  public static final byte[] FAMILY_NAME = TestHRegionFileSystem.FAMILY_NAME;
  private static final byte[][] FAMILIES = {
    Bytes.add(FAMILY_NAME, Bytes.toBytes("-A")), Bytes.add(FAMILY_NAME, Bytes.toBytes("-B"))};
  private static final TableName[] TABLE_NAMES = Stream.of("TestTable", "TestTable2",
          "TestTable3").map(TableName::valueOf).toArray(TableName[]::new);

  private HBaseTestingUtility util = new HBaseTestingUtility();

  private static final Logger LOG = LoggerFactory.getLogger(TestCellBasedHFileOutputFormat2.class);

  /**
   * Simple mapper that makes KeyValue output.
   */
  static class RandomKVGeneratingMapper
      extends Mapper<NullWritable, NullWritable,
                 ImmutableBytesWritable, Cell> {

    private int keyLength;
    private static final int KEYLEN_DEFAULT=10;
    private static final String KEYLEN_CONF="randomkv.key.length";

    private int valLength;
    private static final int VALLEN_DEFAULT=10;
    private static final String VALLEN_CONF="randomkv.val.length";
    private static final byte [] QUALIFIER = Bytes.toBytes("data");
    private boolean multiTableMapper = false;
    private TableName[] tables = null;


    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      super.setup(context);

      Configuration conf = context.getConfiguration();
      keyLength = conf.getInt(KEYLEN_CONF, KEYLEN_DEFAULT);
      valLength = conf.getInt(VALLEN_CONF, VALLEN_DEFAULT);
      multiTableMapper = conf.getBoolean(HFileOutputFormat2.MULTI_TABLE_HFILEOUTPUTFORMAT_CONF_KEY,
              false);
      if (multiTableMapper) {
        tables = TABLE_NAMES;
      } else {
        tables = new TableName[]{TABLE_NAMES[0]};
      }
    }

    @Override
    protected void map(
        NullWritable n1, NullWritable n2,
        Mapper<NullWritable, NullWritable,
               ImmutableBytesWritable,Cell>.Context context)
        throws java.io.IOException ,InterruptedException
    {

      byte keyBytes[] = new byte[keyLength];
      byte valBytes[] = new byte[valLength];

      int taskId = context.getTaskAttemptID().getTaskID().getId();
      assert taskId < Byte.MAX_VALUE : "Unit tests dont support > 127 tasks!";
      Random random = new Random();
      byte[] key;
      for (int j = 0; j < tables.length; ++j) {
        for (int i = 0; i < ROWSPERSPLIT; i++) {
          random.nextBytes(keyBytes);
          // Ensure that unique tasks generate unique keys
          keyBytes[keyLength - 1] = (byte) (taskId & 0xFF);
          random.nextBytes(valBytes);
          key = keyBytes;
          if (multiTableMapper) {
            key = MultiTableHFileOutputFormat.createCompositeKey(tables[j].getName(), keyBytes);
          }

          for (byte[] family : TestCellBasedHFileOutputFormat2.FAMILIES) {
            Cell kv = new KeyValue(keyBytes, family, QUALIFIER, valBytes);
            context.write(new ImmutableBytesWritable(key), kv);
          }
        }
      }
    }
  }

  /**
   * Simple mapper that makes Put output.
   */
  static class RandomPutGeneratingMapper
      extends Mapper<NullWritable, NullWritable,
                 ImmutableBytesWritable, Put> {

    private int keyLength;
    private static final int KEYLEN_DEFAULT = 10;
    private static final String KEYLEN_CONF = "randomkv.key.length";

    private int valLength;
    private static final int VALLEN_DEFAULT = 10;
    private static final String VALLEN_CONF = "randomkv.val.length";
    private static final byte[] QUALIFIER = Bytes.toBytes("data");
    private boolean multiTableMapper = false;
    private TableName[] tables = null;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
      super.setup(context);

      Configuration conf = context.getConfiguration();
      keyLength = conf.getInt(KEYLEN_CONF, KEYLEN_DEFAULT);
      valLength = conf.getInt(VALLEN_CONF, VALLEN_DEFAULT);
      multiTableMapper = conf.getBoolean(HFileOutputFormat2.MULTI_TABLE_HFILEOUTPUTFORMAT_CONF_KEY,
              false);
      if (multiTableMapper) {
        tables = TABLE_NAMES;
      } else {
        tables = new TableName[]{TABLE_NAMES[0]};
      }
    }

    @Override
    protected void map(
            NullWritable n1, NullWritable n2,
            Mapper<NullWritable, NullWritable,
                    ImmutableBytesWritable, Put>.Context context)
            throws java.io.IOException, InterruptedException {

      byte keyBytes[] = new byte[keyLength];
      byte valBytes[] = new byte[valLength];

      int taskId = context.getTaskAttemptID().getTaskID().getId();
      assert taskId < Byte.MAX_VALUE : "Unit tests dont support > 127 tasks!";

      Random random = new Random();
      byte[] key;
      for (int j = 0; j < tables.length; ++j) {
        for (int i = 0; i < ROWSPERSPLIT; i++) {
          random.nextBytes(keyBytes);
          // Ensure that unique tasks generate unique keys
          keyBytes[keyLength - 1] = (byte) (taskId & 0xFF);
          random.nextBytes(valBytes);
          key = keyBytes;
          if (multiTableMapper) {
            key = MultiTableHFileOutputFormat.createCompositeKey(tables[j].getName(), keyBytes);
          }

          for (byte[] family : TestCellBasedHFileOutputFormat2.FAMILIES) {
            Put p = new Put(keyBytes);
            p.addColumn(family, QUALIFIER, valBytes);
            // set TTL to very low so that the scan does not return any value
            p.setTTL(1l);
            context.write(new ImmutableBytesWritable(key), p);
          }
        }
      }
    }
  }

  private void setupRandomGeneratorMapper(Job job, boolean putSortReducer) {
    if (putSortReducer) {
      job.setInputFormatClass(NMapInputFormat.class);
      job.setMapperClass(RandomPutGeneratingMapper.class);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(Put.class);
    } else {
      job.setInputFormatClass(NMapInputFormat.class);
      job.setMapperClass(RandomKVGeneratingMapper.class);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(KeyValue.class);
    }
  }

  /**
   * Test that {@link HFileOutputFormat2} RecordWriter amends timestamps if
   * passed a keyvalue whose timestamp is {@link HConstants#LATEST_TIMESTAMP}.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-2615">HBASE-2615</a>
   */
  @Ignore("Goes zombie too frequently; needs work. See HBASE-14563") @Test
  public void test_LATEST_TIMESTAMP_isReplaced()
  throws Exception {
    Configuration conf = new Configuration(this.util.getConfiguration());
    RecordWriter<ImmutableBytesWritable, Cell> writer = null;
    TaskAttemptContext context = null;
    Path dir =
      util.getDataTestDir("test_LATEST_TIMESTAMP_isReplaced");
    try {
      Job job = new Job(conf);
      FileOutputFormat.setOutputPath(job, dir);
      context = createTestTaskAttemptContext(job);
      HFileOutputFormat2 hof = new HFileOutputFormat2();
      writer = hof.getRecordWriter(context);
      final byte [] b = Bytes.toBytes("b");

      // Test 1.  Pass a KV that has a ts of LATEST_TIMESTAMP.  It should be
      // changed by call to write.  Check all in kv is same but ts.
      KeyValue kv = new KeyValue(b, b, b);
      KeyValue original = kv.clone();
      writer.write(new ImmutableBytesWritable(), kv);
      assertFalse(original.equals(kv));
      assertTrue(Bytes.equals(CellUtil.cloneRow(original), CellUtil.cloneRow(kv)));
      assertTrue(Bytes.equals(CellUtil.cloneFamily(original), CellUtil.cloneFamily(kv)));
      assertTrue(Bytes.equals(CellUtil.cloneQualifier(original), CellUtil.cloneQualifier(kv)));
      assertNotSame(original.getTimestamp(), kv.getTimestamp());
      assertNotSame(HConstants.LATEST_TIMESTAMP, kv.getTimestamp());

      // Test 2. Now test passing a kv that has explicit ts.  It should not be
      // changed by call to record write.
      kv = new KeyValue(b, b, b, kv.getTimestamp() - 1, b);
      original = kv.clone();
      writer.write(new ImmutableBytesWritable(), kv);
      assertTrue(original.equals(kv));
    } finally {
      if (writer != null && context != null) writer.close(context);
      dir.getFileSystem(conf).delete(dir, true);
    }
  }

  private TaskAttemptContext createTestTaskAttemptContext(final Job job)
  throws Exception {
    HadoopShims hadoop = CompatibilitySingletonFactory.getInstance(HadoopShims.class);
    TaskAttemptContext context = hadoop.createTestTaskAttemptContext(
      job, "attempt_201402131733_0001_m_000000_0");
    return context;
  }

  /*
   * Test that {@link HFileOutputFormat2} creates an HFile with TIMERANGE
   * metadata used by time-restricted scans.
   */
  @Ignore("Goes zombie too frequently; needs work. See HBASE-14563") @Test
  public void test_TIMERANGE() throws Exception {
    Configuration conf = new Configuration(this.util.getConfiguration());
    RecordWriter<ImmutableBytesWritable, Cell> writer = null;
    TaskAttemptContext context = null;
    Path dir =
      util.getDataTestDir("test_TIMERANGE_present");
    LOG.info("Timerange dir writing to dir: "+ dir);
    try {
      // build a record writer using HFileOutputFormat2
      Job job = new Job(conf);
      FileOutputFormat.setOutputPath(job, dir);
      context = createTestTaskAttemptContext(job);
      HFileOutputFormat2 hof = new HFileOutputFormat2();
      writer = hof.getRecordWriter(context);

      // Pass two key values with explicit times stamps
      final byte [] b = Bytes.toBytes("b");

      // value 1 with timestamp 2000
      KeyValue kv = new KeyValue(b, b, b, 2000, b);
      KeyValue original = kv.clone();
      writer.write(new ImmutableBytesWritable(), kv);
      assertEquals(original,kv);

      // value 2 with timestamp 1000
      kv = new KeyValue(b, b, b, 1000, b);
      original = kv.clone();
      writer.write(new ImmutableBytesWritable(), kv);
      assertEquals(original, kv);

      // verify that the file has the proper FileInfo.
      writer.close(context);

      // the generated file lives 1 directory down from the attempt directory
      // and is the only file, e.g.
      // _attempt__0000_r_000000_0/b/1979617994050536795
      FileSystem fs = FileSystem.get(conf);
      Path attemptDirectory = hof.getDefaultWorkFile(context, "").getParent();
      FileStatus[] sub1 = fs.listStatus(attemptDirectory);
      FileStatus[] file = fs.listStatus(sub1[0].getPath());

      // open as HFile Reader and pull out TIMERANGE FileInfo.
      HFile.Reader rd =
          HFile.createReader(fs, file[0].getPath(), new CacheConfig(conf), true, conf);
      Map<byte[],byte[]> finfo = rd.getHFileInfo();
      byte[] range = finfo.get("TIMERANGE".getBytes("UTF-8"));
      assertNotNull(range);

      // unmarshall and check values.
      TimeRangeTracker timeRangeTracker = TimeRangeTracker.parseFrom(range);
      LOG.info(timeRangeTracker.getMin() +
          "...." + timeRangeTracker.getMax());
      assertEquals(1000, timeRangeTracker.getMin());
      assertEquals(2000, timeRangeTracker.getMax());
      rd.close();
    } finally {
      if (writer != null && context != null) writer.close(context);
      dir.getFileSystem(conf).delete(dir, true);
    }
  }

  /**
   * Run small MR job.
   */
  @Ignore("Goes zombie too frequently; needs work. See HBASE-14563") @Test
  public void testWritingPEData() throws Exception {
    Configuration conf = util.getConfiguration();
    Path testDir = util.getDataTestDirOnTestFS("testWritingPEData");
    FileSystem fs = testDir.getFileSystem(conf);

    // Set down this value or we OOME in eclipse.
    conf.setInt("mapreduce.task.io.sort.mb", 20);
    // Write a few files.
    conf.setLong(HConstants.HREGION_MAX_FILESIZE, 64 * 1024);

    Job job = new Job(conf, "testWritingPEData");
    setupRandomGeneratorMapper(job, false);
    // This partitioner doesn't work well for number keys but using it anyways
    // just to demonstrate how to configure it.
    byte[] startKey = new byte[RandomKVGeneratingMapper.KEYLEN_DEFAULT];
    byte[] endKey = new byte[RandomKVGeneratingMapper.KEYLEN_DEFAULT];

    Arrays.fill(startKey, (byte)0);
    Arrays.fill(endKey, (byte)0xff);

    job.setPartitionerClass(SimpleTotalOrderPartitioner.class);
    // Set start and end rows for partitioner.
    SimpleTotalOrderPartitioner.setStartKey(job.getConfiguration(), startKey);
    SimpleTotalOrderPartitioner.setEndKey(job.getConfiguration(), endKey);
    job.setReducerClass(CellSortReducer.class);
    job.setOutputFormatClass(HFileOutputFormat2.class);
    job.setNumReduceTasks(4);
    job.getConfiguration().setStrings("io.serializations", conf.get("io.serializations"),
        MutationSerialization.class.getName(), ResultSerialization.class.getName(),
        CellSerialization.class.getName());

    FileOutputFormat.setOutputPath(job, testDir);
    assertTrue(job.waitForCompletion(false));
    FileStatus [] files = fs.listStatus(testDir);
    assertTrue(files.length > 0);
  }

  /**
   * Test that {@link HFileOutputFormat2} RecordWriter writes tags such as ttl into
   * hfile.
   */
  @Test
  public void test_WritingTagData()
      throws Exception {
    Configuration conf = new Configuration(this.util.getConfiguration());
    final String HFILE_FORMAT_VERSION_CONF_KEY = "hfile.format.version";
    conf.setInt(HFILE_FORMAT_VERSION_CONF_KEY, HFile.MIN_FORMAT_VERSION_WITH_TAGS);
    RecordWriter<ImmutableBytesWritable, Cell> writer = null;
    TaskAttemptContext context = null;
    Path dir =
        util.getDataTestDir("WritingTagData");
    try {
      conf.set(HFileOutputFormat2.OUTPUT_TABLE_NAME_CONF_KEY, TABLE_NAMES[0].getNameAsString());
      // turn locality off to eliminate getRegionLocation fail-and-retry time when writing kvs
      conf.setBoolean(HFileOutputFormat2.LOCALITY_SENSITIVE_CONF_KEY, false);
      Job job = new Job(conf);
      FileOutputFormat.setOutputPath(job, dir);
      context = createTestTaskAttemptContext(job);
      HFileOutputFormat2 hof = new HFileOutputFormat2();
      writer = hof.getRecordWriter(context);
      final byte [] b = Bytes.toBytes("b");

      List< Tag > tags = new ArrayList<>();
      tags.add(new ArrayBackedTag(TagType.TTL_TAG_TYPE, Bytes.toBytes(978670)));
      KeyValue kv = new KeyValue(b, b, b, HConstants.LATEST_TIMESTAMP, b, tags);
      writer.write(new ImmutableBytesWritable(), kv);
      writer.close(context);
      writer = null;
      FileSystem fs = dir.getFileSystem(conf);
      RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(dir, true);
      while(iterator.hasNext()) {
        LocatedFileStatus keyFileStatus = iterator.next();
        HFile.Reader reader =
            HFile.createReader(fs, keyFileStatus.getPath(), new CacheConfig(conf), true, conf);
        HFileScanner scanner = reader.getScanner(false, false, false);
        scanner.seekTo();
        Cell cell = scanner.getCell();
        List<Tag> tagsFromCell = TagUtil.asList(cell.getTagsArray(), cell.getTagsOffset(),
            cell.getTagsLength());
        assertTrue(tagsFromCell.size() > 0);
        for (Tag tag : tagsFromCell) {
          assertTrue(tag.getType() == TagType.TTL_TAG_TYPE);
        }
      }
    } finally {
      if (writer != null && context != null) writer.close(context);
      dir.getFileSystem(conf).delete(dir, true);
    }
  }

  @Ignore("Goes zombie too frequently; needs work. See HBASE-14563") @Test
  public void testJobConfiguration() throws Exception {
    Configuration conf = new Configuration(this.util.getConfiguration());
    conf.set(HConstants.TEMPORARY_FS_DIRECTORY_KEY, util.getDataTestDir("testJobConfiguration")
        .toString());
    Job job = new Job(conf);
    job.setWorkingDirectory(util.getDataTestDir("testJobConfiguration"));
    Table table = Mockito.mock(Table.class);
    RegionLocator regionLocator = Mockito.mock(RegionLocator.class);
    setupMockStartKeys(regionLocator);
    setupMockTableName(regionLocator);
    HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor(), regionLocator);
    assertEquals(job.getNumReduceTasks(), 4);
  }

  private byte [][] generateRandomStartKeys(int numKeys) {
    Random random = new Random();
    byte[][] ret = new byte[numKeys][];
    // first region start key is always empty
    ret[0] = HConstants.EMPTY_BYTE_ARRAY;
    for (int i = 1; i < numKeys; i++) {
      ret[i] =
        PerformanceEvaluation.generateData(random, PerformanceEvaluation.DEFAULT_VALUE_LENGTH);
    }
    return ret;
  }

  private byte[][] generateRandomSplitKeys(int numKeys) {
    Random random = new Random();
    byte[][] ret = new byte[numKeys][];
    for (int i = 0; i < numKeys; i++) {
      ret[i] =
          PerformanceEvaluation.generateData(random, PerformanceEvaluation.DEFAULT_VALUE_LENGTH);
    }
    return ret;
  }

  @Ignore("Goes zombie too frequently; needs work. See HBASE-14563") @Test
  public void testMRIncrementalLoad() throws Exception {
    LOG.info("\nStarting test testMRIncrementalLoad\n");
    doIncrementalLoadTest(false, false, false, "testMRIncrementalLoad");
  }

  @Ignore("Goes zombie too frequently; needs work. See HBASE-14563") @Test
  public void testMRIncrementalLoadWithSplit() throws Exception {
    LOG.info("\nStarting test testMRIncrementalLoadWithSplit\n");
    doIncrementalLoadTest(true, false, false, "testMRIncrementalLoadWithSplit");
  }

  /**
   * Test for HFileOutputFormat2.LOCALITY_SENSITIVE_CONF_KEY = true
   * This test could only check the correctness of original logic if LOCALITY_SENSITIVE_CONF_KEY
   * is set to true. Because MiniHBaseCluster always run with single hostname (and different ports),
   * it's not possible to check the region locality by comparing region locations and DN hostnames.
   * When MiniHBaseCluster supports explicit hostnames parameter (just like MiniDFSCluster does),
   * we could test region locality features more easily.
   */
  @Ignore("Goes zombie too frequently; needs work. See HBASE-14563") @Test
  public void testMRIncrementalLoadWithLocality() throws Exception {
    LOG.info("\nStarting test testMRIncrementalLoadWithLocality\n");
    doIncrementalLoadTest(false, true, false, "testMRIncrementalLoadWithLocality1");
    doIncrementalLoadTest(true, true, false, "testMRIncrementalLoadWithLocality2");
  }

  //@Ignore("Wahtevs")
  @Test
  public void testMRIncrementalLoadWithPutSortReducer() throws Exception {
    LOG.info("\nStarting test testMRIncrementalLoadWithPutSortReducer\n");
    doIncrementalLoadTest(false, false, true, "testMRIncrementalLoadWithPutSortReducer");
  }

  private void doIncrementalLoadTest(boolean shouldChangeRegions, boolean shouldKeepLocality,
                                     boolean putSortReducer, String tableStr) throws Exception {
      doIncrementalLoadTest(shouldChangeRegions, shouldKeepLocality, putSortReducer,
              Arrays.asList(tableStr));
  }

  @Test
  public void testMultiMRIncrementalLoadWithPutSortReducer() throws Exception {
    LOG.info("\nStarting test testMultiMRIncrementalLoadWithPutSortReducer\n");
    doIncrementalLoadTest(false, false, true,
            Arrays.stream(TABLE_NAMES).map(TableName::getNameAsString).collect(Collectors.toList
                    ()));
  }

  private void doIncrementalLoadTest(boolean shouldChangeRegions, boolean shouldKeepLocality,
      boolean putSortReducer, List<String> tableStr) throws Exception {
    util = new HBaseTestingUtility();
    Configuration conf = util.getConfiguration();
    conf.setBoolean(MultiTableHFileOutputFormat.LOCALITY_SENSITIVE_CONF_KEY, shouldKeepLocality);
    int hostCount = 1;
    int regionNum = 5;
    if (shouldKeepLocality) {
      // We should change host count higher than hdfs replica count when MiniHBaseCluster supports
      // explicit hostnames parameter just like MiniDFSCluster does.
      hostCount = 3;
      regionNum = 20;
    }

    String[] hostnames = new String[hostCount];
    for (int i = 0; i < hostCount; ++i) {
      hostnames[i] = "datanode_" + i;
    }
    util.startMiniCluster(1, hostCount, hostnames);

    Map<String, Table> allTables = new HashMap<>(tableStr.size());
    List<HFileOutputFormat2.TableInfo> tableInfo = new ArrayList<>(tableStr.size());
    boolean writeMultipleTables = tableStr.size() > 1;
    for (String tableStrSingle : tableStr) {
      byte[][] splitKeys = generateRandomSplitKeys(regionNum - 1);
      TableName tableName = TableName.valueOf(tableStrSingle);
      Table table = util.createTable(tableName, FAMILIES, splitKeys);

      RegionLocator r = util.getConnection().getRegionLocator(tableName);
      assertEquals("Should start with empty table", 0, util.countRows(table));
      int numRegions = r.getStartKeys().length;
      assertEquals("Should make " + regionNum + " regions", numRegions, regionNum);

      allTables.put(tableStrSingle, table);
      tableInfo.add(new HFileOutputFormat2.TableInfo(table.getTableDescriptor(), r));
    }
    Path testDir = util.getDataTestDirOnTestFS("testLocalMRIncrementalLoad");
    // Generate the bulk load files
    runIncrementalPELoad(conf, tableInfo, testDir, putSortReducer);

    for (Table tableSingle : allTables.values()) {
      // This doesn't write into the table, just makes files
      assertEquals("HFOF should not touch actual table", 0, util.countRows(tableSingle));
    }
    int numTableDirs = 0;
    for (FileStatus tf : testDir.getFileSystem(conf).listStatus(testDir)) {
      Path tablePath = testDir;

      if (writeMultipleTables) {
        if (allTables.containsKey(tf.getPath().getName())) {
          ++numTableDirs;
          tablePath = tf.getPath();
        }
        else {
          continue;
        }
      }

      // Make sure that a directory was created for every CF
      int dir = 0;
      for (FileStatus f : tablePath.getFileSystem(conf).listStatus(tablePath)) {
        for (byte[] family : FAMILIES) {
          if (Bytes.toString(family).equals(f.getPath().getName())) {
            ++dir;
          }
        }
      }
      assertEquals("Column family not found in FS.", FAMILIES.length, dir);
    }
    if (writeMultipleTables) {
      assertEquals("Dir for all input tables not created", numTableDirs, allTables.size());
    }

    Admin admin = util.getConnection().getAdmin();
    try {
      // handle the split case
      if (shouldChangeRegions) {
        Table chosenTable = allTables.values().iterator().next();
        // Choose a semi-random table if multiple tables are available
        LOG.info("Changing regions in table " + chosenTable.getName().getNameAsString());
        admin.disableTable(chosenTable.getName());
        util.waitUntilNoRegionsInTransition();

        util.deleteTable(chosenTable.getName());
        byte[][] newSplitKeys = generateRandomSplitKeys(14);
        Table table = util.createTable(chosenTable.getName(), FAMILIES, newSplitKeys);

        while (util.getConnection().getRegionLocator(chosenTable.getName())
                .getAllRegionLocations().size() != 15 ||
                !admin.isTableAvailable(table.getName())) {
          Thread.sleep(200);
          LOG.info("Waiting for new region assignment to happen");
        }
      }

      // Perform the actual load
      for (HFileOutputFormat2.TableInfo singleTableInfo : tableInfo) {
        Path tableDir = testDir;
        String tableNameStr = singleTableInfo.getHTableDescriptor().getNameAsString();
        LOG.info("Running LoadIncrementalHFiles on table" + tableNameStr);
        if (writeMultipleTables) {
          tableDir = new Path(testDir, tableNameStr);
        }
        Table currentTable = allTables.get(tableNameStr);
        TableName currentTableName = currentTable.getName();
        new LoadIncrementalHFiles(conf).doBulkLoad(tableDir, admin, currentTable, singleTableInfo
                .getRegionLocator());

        // Ensure data shows up
        int expectedRows = 0;
        if (putSortReducer) {
          // no rows should be extracted
          assertEquals("LoadIncrementalHFiles should put expected data in table", expectedRows,
                  util.countRows(currentTable));
        } else {
          expectedRows = NMapInputFormat.getNumMapTasks(conf) * ROWSPERSPLIT;
          assertEquals("LoadIncrementalHFiles should put expected data in table", expectedRows,
                  util.countRows(currentTable));
          Scan scan = new Scan();
          ResultScanner results = currentTable.getScanner(scan);
          for (Result res : results) {
            assertEquals(FAMILIES.length, res.rawCells().length);
            Cell first = res.rawCells()[0];
            for (Cell kv : res.rawCells()) {
              assertTrue(CellUtil.matchingRows(first, kv));
              assertTrue(Bytes.equals(CellUtil.cloneValue(first), CellUtil.cloneValue(kv)));
            }
          }
          results.close();
        }
        String tableDigestBefore = util.checksumRows(currentTable);
        // Check region locality
        HDFSBlocksDistribution hbd = new HDFSBlocksDistribution();
        for (HRegion region : util.getHBaseCluster().getRegions(currentTableName)) {
          hbd.add(region.getHDFSBlocksDistribution());
        }
        for (String hostname : hostnames) {
          float locality = hbd.getBlockLocalityIndex(hostname);
          LOG.info("locality of [" + hostname + "]: " + locality);
          assertEquals(100, (int) (locality * 100));
        }

        // Cause regions to reopen
        admin.disableTable(currentTableName);
        while (!admin.isTableDisabled(currentTableName)) {
          Thread.sleep(200);
          LOG.info("Waiting for table to disable");
        }
        admin.enableTable(currentTableName);
        util.waitTableAvailable(currentTableName);
        assertEquals("Data should remain after reopening of regions",
                tableDigestBefore, util.checksumRows(currentTable));
      }
    } finally {
      for (HFileOutputFormat2.TableInfo tableInfoSingle : tableInfo) {
          tableInfoSingle.getRegionLocator().close();
      }
      for (Entry<String, Table> singleTable : allTables.entrySet() ) {
        singleTable.getValue().close();
        util.deleteTable(singleTable.getValue().getName());
      }
      testDir.getFileSystem(conf).delete(testDir, true);
      util.shutdownMiniCluster();
    }
  }

  private void runIncrementalPELoad(Configuration conf, List<HFileOutputFormat2.TableInfo> tableInfo, Path outDir,
                                    boolean putSortReducer) throws IOException,
          InterruptedException, ClassNotFoundException {
    Job job = new Job(conf, "testLocalMRIncrementalLoad");
    job.setWorkingDirectory(util.getDataTestDirOnTestFS("runIncrementalPELoad"));
    job.getConfiguration().setStrings("io.serializations", conf.get("io.serializations"),
        MutationSerialization.class.getName(), ResultSerialization.class.getName(),
        CellSerialization.class.getName());
    setupRandomGeneratorMapper(job, putSortReducer);
    if (tableInfo.size() > 1) {
      MultiTableHFileOutputFormat.configureIncrementalLoad(job, tableInfo);
      int sum = 0;
      for (HFileOutputFormat2.TableInfo tableInfoSingle : tableInfo) {
        sum += tableInfoSingle.getRegionLocator().getAllRegionLocations().size();
      }
      assertEquals(sum, job.getNumReduceTasks());
    }
    else {
      RegionLocator regionLocator = tableInfo.get(0).getRegionLocator();
      HFileOutputFormat2.configureIncrementalLoad(job, tableInfo.get(0).getHTableDescriptor(),
              regionLocator);
      assertEquals(regionLocator.getAllRegionLocations().size(), job.getNumReduceTasks());
    }

    FileOutputFormat.setOutputPath(job, outDir);

    assertFalse(util.getTestFileSystem().exists(outDir)) ;

    assertTrue(job.waitForCompletion(true));
  }

  /**
   * Test for {@link HFileOutputFormat2#configureCompression(Configuration, HTableDescriptor)} and
   * {@link HFileOutputFormat2#createFamilyCompressionMap(Configuration)}.
   * Tests that the compression map is correctly serialized into
   * and deserialized from configuration
   *
   * @throws IOException
   */
  @Ignore("Goes zombie too frequently; needs work. See HBASE-14563") @Test
  public void testSerializeDeserializeFamilyCompressionMap() throws IOException {
    for (int numCfs = 0; numCfs <= 3; numCfs++) {
      Configuration conf = new Configuration(this.util.getConfiguration());
      Map<String, Compression.Algorithm> familyToCompression =
          getMockColumnFamiliesForCompression(numCfs);
      Table table = Mockito.mock(Table.class);
      setupMockColumnFamiliesForCompression(table, familyToCompression);
      conf.set(HFileOutputFormat2.COMPRESSION_FAMILIES_CONF_KEY,
              HFileOutputFormat2.serializeColumnFamilyAttribute
                      (HFileOutputFormat2.compressionDetails,
                              Arrays.asList(table.getTableDescriptor())));

      // read back family specific compression setting from the configuration
      Map<byte[], Algorithm> retrievedFamilyToCompressionMap = HFileOutputFormat2
          .createFamilyCompressionMap(conf);

      // test that we have a value for all column families that matches with the
      // used mock values
      for (Entry<String, Algorithm> entry : familyToCompression.entrySet()) {
        assertEquals("Compression configuration incorrect for column family:"
            + entry.getKey(), entry.getValue(),
            retrievedFamilyToCompressionMap.get(entry.getKey().getBytes("UTF-8")));
      }
    }
  }

  private void setupMockColumnFamiliesForCompression(Table table,
      Map<String, Compression.Algorithm> familyToCompression) throws IOException {
    HTableDescriptor mockTableDescriptor = new HTableDescriptor(TABLE_NAMES[0]);
    for (Entry<String, Compression.Algorithm> entry : familyToCompression.entrySet()) {
      mockTableDescriptor.addFamily(new HColumnDescriptor(entry.getKey())
          .setMaxVersions(1)
          .setCompressionType(entry.getValue())
          .setBlockCacheEnabled(false)
          .setTimeToLive(0));
    }
    Mockito.doReturn(mockTableDescriptor).when(table).getTableDescriptor();
  }

  /**
   * @return a map from column family names to compression algorithms for
   *         testing column family compression. Column family names have special characters
   */
  private Map<String, Compression.Algorithm>
      getMockColumnFamiliesForCompression (int numCfs) {
    Map<String, Compression.Algorithm> familyToCompression = new HashMap<>();
    // use column family names having special characters
    if (numCfs-- > 0) {
      familyToCompression.put("Family1!@#!@#&", Compression.Algorithm.LZO);
    }
    if (numCfs-- > 0) {
      familyToCompression.put("Family2=asdads&!AASD", Compression.Algorithm.SNAPPY);
    }
    if (numCfs-- > 0) {
      familyToCompression.put("Family2=asdads&!AASD", Compression.Algorithm.GZ);
    }
    if (numCfs-- > 0) {
      familyToCompression.put("Family3", Compression.Algorithm.NONE);
    }
    return familyToCompression;
  }


  /**
   * Test for {@link HFileOutputFormat2#configureBloomType(HTableDescriptor, Configuration)} and
   * {@link HFileOutputFormat2#createFamilyBloomTypeMap(Configuration)}.
   * Tests that the compression map is correctly serialized into
   * and deserialized from configuration
   *
   * @throws IOException
   */
  @Ignore("Goes zombie too frequently; needs work. See HBASE-14563") @Test
  public void testSerializeDeserializeFamilyBloomTypeMap() throws IOException {
    for (int numCfs = 0; numCfs <= 2; numCfs++) {
      Configuration conf = new Configuration(this.util.getConfiguration());
      Map<String, BloomType> familyToBloomType =
          getMockColumnFamiliesForBloomType(numCfs);
      Table table = Mockito.mock(Table.class);
      setupMockColumnFamiliesForBloomType(table,
          familyToBloomType);
      conf.set(HFileOutputFormat2.BLOOM_TYPE_FAMILIES_CONF_KEY,
              HFileOutputFormat2.serializeColumnFamilyAttribute(HFileOutputFormat2.bloomTypeDetails,
              Arrays.asList(table.getTableDescriptor())));

      // read back family specific data block encoding settings from the
      // configuration
      Map<byte[], BloomType> retrievedFamilyToBloomTypeMap =
          HFileOutputFormat2
              .createFamilyBloomTypeMap(conf);

      // test that we have a value for all column families that matches with the
      // used mock values
      for (Entry<String, BloomType> entry : familyToBloomType.entrySet()) {
        assertEquals("BloomType configuration incorrect for column family:"
            + entry.getKey(), entry.getValue(),
            retrievedFamilyToBloomTypeMap.get(entry.getKey().getBytes("UTF-8")));
      }
    }
  }

  private void setupMockColumnFamiliesForBloomType(Table table,
      Map<String, BloomType> familyToDataBlockEncoding) throws IOException {
    HTableDescriptor mockTableDescriptor = new HTableDescriptor(TABLE_NAMES[0]);
    for (Entry<String, BloomType> entry : familyToDataBlockEncoding.entrySet()) {
      mockTableDescriptor.addFamily(new HColumnDescriptor(entry.getKey())
          .setMaxVersions(1)
          .setBloomFilterType(entry.getValue())
          .setBlockCacheEnabled(false)
          .setTimeToLive(0));
    }
    Mockito.doReturn(mockTableDescriptor).when(table).getTableDescriptor();
  }

  /**
   * @return a map from column family names to compression algorithms for
   *         testing column family compression. Column family names have special characters
   */
  private Map<String, BloomType>
  getMockColumnFamiliesForBloomType (int numCfs) {
    Map<String, BloomType> familyToBloomType = new HashMap<>();
    // use column family names having special characters
    if (numCfs-- > 0) {
      familyToBloomType.put("Family1!@#!@#&", BloomType.ROW);
    }
    if (numCfs-- > 0) {
      familyToBloomType.put("Family2=asdads&!AASD",
          BloomType.ROWCOL);
    }
    if (numCfs-- > 0) {
      familyToBloomType.put("Family3", BloomType.NONE);
    }
    return familyToBloomType;
  }

  /**
   * Test for {@link HFileOutputFormat2#configureBlockSize(HTableDescriptor, Configuration)} and
   * {@link HFileOutputFormat2#createFamilyBlockSizeMap(Configuration)}.
   * Tests that the compression map is correctly serialized into
   * and deserialized from configuration
   *
   * @throws IOException
   */
  @Ignore("Goes zombie too frequently; needs work. See HBASE-14563") @Test
  public void testSerializeDeserializeFamilyBlockSizeMap() throws IOException {
    for (int numCfs = 0; numCfs <= 3; numCfs++) {
      Configuration conf = new Configuration(this.util.getConfiguration());
      Map<String, Integer> familyToBlockSize =
          getMockColumnFamiliesForBlockSize(numCfs);
      Table table = Mockito.mock(Table.class);
      setupMockColumnFamiliesForBlockSize(table,
          familyToBlockSize);
      conf.set(HFileOutputFormat2.BLOCK_SIZE_FAMILIES_CONF_KEY,
              HFileOutputFormat2.serializeColumnFamilyAttribute
                      (HFileOutputFormat2.blockSizeDetails, Arrays.asList(table
                              .getTableDescriptor())));

      // read back family specific data block encoding settings from the
      // configuration
      Map<byte[], Integer> retrievedFamilyToBlockSizeMap =
          HFileOutputFormat2
              .createFamilyBlockSizeMap(conf);

      // test that we have a value for all column families that matches with the
      // used mock values
      for (Entry<String, Integer> entry : familyToBlockSize.entrySet()
          ) {
        assertEquals("BlockSize configuration incorrect for column family:"
            + entry.getKey(), entry.getValue(),
            retrievedFamilyToBlockSizeMap.get(entry.getKey().getBytes("UTF-8")));
      }
    }
  }

  private void setupMockColumnFamiliesForBlockSize(Table table,
      Map<String, Integer> familyToDataBlockEncoding) throws IOException {
    HTableDescriptor mockTableDescriptor = new HTableDescriptor(TABLE_NAMES[0]);
    for (Entry<String, Integer> entry : familyToDataBlockEncoding.entrySet()) {
      mockTableDescriptor.addFamily(new HColumnDescriptor(entry.getKey())
          .setMaxVersions(1)
          .setBlocksize(entry.getValue())
          .setBlockCacheEnabled(false)
          .setTimeToLive(0));
    }
    Mockito.doReturn(mockTableDescriptor).when(table).getTableDescriptor();
  }

  /**
   * @return a map from column family names to compression algorithms for
   *         testing column family compression. Column family names have special characters
   */
  private Map<String, Integer>
  getMockColumnFamiliesForBlockSize (int numCfs) {
    Map<String, Integer> familyToBlockSize = new HashMap<>();
    // use column family names having special characters
    if (numCfs-- > 0) {
      familyToBlockSize.put("Family1!@#!@#&", 1234);
    }
    if (numCfs-- > 0) {
      familyToBlockSize.put("Family2=asdads&!AASD",
          Integer.MAX_VALUE);
    }
    if (numCfs-- > 0) {
      familyToBlockSize.put("Family2=asdads&!AASD",
          Integer.MAX_VALUE);
    }
    if (numCfs-- > 0) {
      familyToBlockSize.put("Family3", 0);
    }
    return familyToBlockSize;
  }

  /**
   * Test for {@link HFileOutputFormat2#configureDataBlockEncoding(HTableDescriptor, Configuration)}
   * and {@link HFileOutputFormat2#createFamilyDataBlockEncodingMap(Configuration)}.
   * Tests that the compression map is correctly serialized into
   * and deserialized from configuration
   *
   * @throws IOException
   */
  @Ignore("Goes zombie too frequently; needs work. See HBASE-14563") @Test
  public void testSerializeDeserializeFamilyDataBlockEncodingMap() throws IOException {
    for (int numCfs = 0; numCfs <= 3; numCfs++) {
      Configuration conf = new Configuration(this.util.getConfiguration());
      Map<String, DataBlockEncoding> familyToDataBlockEncoding =
          getMockColumnFamiliesForDataBlockEncoding(numCfs);
      Table table = Mockito.mock(Table.class);
      setupMockColumnFamiliesForDataBlockEncoding(table,
          familyToDataBlockEncoding);
      HTableDescriptor tableDescriptor = table.getTableDescriptor();
      conf.set(HFileOutputFormat2.DATABLOCK_ENCODING_FAMILIES_CONF_KEY,
              HFileOutputFormat2.serializeColumnFamilyAttribute
                      (HFileOutputFormat2.dataBlockEncodingDetails, Arrays
                      .asList(tableDescriptor)));

      // read back family specific data block encoding settings from the
      // configuration
      Map<byte[], DataBlockEncoding> retrievedFamilyToDataBlockEncodingMap =
          HFileOutputFormat2
          .createFamilyDataBlockEncodingMap(conf);

      // test that we have a value for all column families that matches with the
      // used mock values
      for (Entry<String, DataBlockEncoding> entry : familyToDataBlockEncoding.entrySet()) {
        assertEquals("DataBlockEncoding configuration incorrect for column family:"
            + entry.getKey(), entry.getValue(),
            retrievedFamilyToDataBlockEncodingMap.get(entry.getKey().getBytes("UTF-8")));
      }
    }
  }

  private void setupMockColumnFamiliesForDataBlockEncoding(Table table,
      Map<String, DataBlockEncoding> familyToDataBlockEncoding) throws IOException {
    HTableDescriptor mockTableDescriptor = new HTableDescriptor(TABLE_NAMES[0]);
    for (Entry<String, DataBlockEncoding> entry : familyToDataBlockEncoding.entrySet()) {
      mockTableDescriptor.addFamily(new HColumnDescriptor(entry.getKey())
          .setMaxVersions(1)
          .setDataBlockEncoding(entry.getValue())
          .setBlockCacheEnabled(false)
          .setTimeToLive(0));
    }
    Mockito.doReturn(mockTableDescriptor).when(table).getTableDescriptor();
  }

  /**
   * @return a map from column family names to compression algorithms for
   *         testing column family compression. Column family names have special characters
   */
  private Map<String, DataBlockEncoding>
      getMockColumnFamiliesForDataBlockEncoding (int numCfs) {
    Map<String, DataBlockEncoding> familyToDataBlockEncoding = new HashMap<>();
    // use column family names having special characters
    if (numCfs-- > 0) {
      familyToDataBlockEncoding.put("Family1!@#!@#&", DataBlockEncoding.DIFF);
    }
    if (numCfs-- > 0) {
      familyToDataBlockEncoding.put("Family2=asdads&!AASD",
          DataBlockEncoding.FAST_DIFF);
    }
    if (numCfs-- > 0) {
      familyToDataBlockEncoding.put("Family2=asdads&!AASD",
          DataBlockEncoding.PREFIX);
    }
    if (numCfs-- > 0) {
      familyToDataBlockEncoding.put("Family3", DataBlockEncoding.NONE);
    }
    return familyToDataBlockEncoding;
  }

  private void setupMockStartKeys(RegionLocator table) throws IOException {
    byte[][] mockKeys = new byte[][] {
        HConstants.EMPTY_BYTE_ARRAY,
        Bytes.toBytes("aaa"),
        Bytes.toBytes("ggg"),
        Bytes.toBytes("zzz")
    };
    Mockito.doReturn(mockKeys).when(table).getStartKeys();
  }

  private void setupMockTableName(RegionLocator table) throws IOException {
    TableName mockTableName = TableName.valueOf("mock_table");
    Mockito.doReturn(mockTableName).when(table).getName();
  }

  /**
   * Test that {@link HFileOutputFormat2} RecordWriter uses compression and
   * bloom filter settings from the column family descriptor
   */
  @Ignore("Goes zombie too frequently; needs work. See HBASE-14563") @Test
  public void testColumnFamilySettings() throws Exception {
    Configuration conf = new Configuration(this.util.getConfiguration());
    RecordWriter<ImmutableBytesWritable, Cell> writer = null;
    TaskAttemptContext context = null;
    Path dir = util.getDataTestDir("testColumnFamilySettings");

    // Setup table descriptor
    Table table = Mockito.mock(Table.class);
    RegionLocator regionLocator = Mockito.mock(RegionLocator.class);
    HTableDescriptor htd = new HTableDescriptor(TABLE_NAMES[0]);
    Mockito.doReturn(htd).when(table).getTableDescriptor();
    for (HColumnDescriptor hcd: HBaseTestingUtility.generateColumnDescriptors()) {
      htd.addFamily(hcd);
    }

    // set up the table to return some mock keys
    setupMockStartKeys(regionLocator);

    try {
      // partial map red setup to get an operational writer for testing
      // We turn off the sequence file compression, because DefaultCodec
      // pollutes the GZip codec pool with an incompatible compressor.
      conf.set("io.seqfile.compression.type", "NONE");
      conf.set("hbase.fs.tmp.dir", dir.toString());
      // turn locality off to eliminate getRegionLocation fail-and-retry time when writing kvs
      conf.setBoolean(HFileOutputFormat2.LOCALITY_SENSITIVE_CONF_KEY, false);

      Job job = new Job(conf, "testLocalMRIncrementalLoad");
      job.setWorkingDirectory(util.getDataTestDirOnTestFS("testColumnFamilySettings"));
      setupRandomGeneratorMapper(job, false);
      HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor(), regionLocator);
      FileOutputFormat.setOutputPath(job, dir);
      context = createTestTaskAttemptContext(job);
      HFileOutputFormat2 hof = new HFileOutputFormat2();
      writer = hof.getRecordWriter(context);

      // write out random rows
      writeRandomKeyValues(writer, context, htd.getFamiliesKeys(), ROWSPERSPLIT);
      writer.close(context);

      // Make sure that a directory was created for every CF
      FileSystem fs = dir.getFileSystem(conf);

      // commit so that the filesystem has one directory per column family
      hof.getOutputCommitter(context).commitTask(context);
      hof.getOutputCommitter(context).commitJob(context);
      FileStatus[] families = CommonFSUtils.listStatus(fs, dir, new FSUtils.FamilyDirFilter(fs));
      assertEquals(htd.getFamilies().size(), families.length);
      for (FileStatus f : families) {
        String familyStr = f.getPath().getName();
        HColumnDescriptor hcd = htd.getFamily(Bytes.toBytes(familyStr));
        // verify that the compression on this file matches the configured
        // compression
        Path dataFilePath = fs.listStatus(f.getPath())[0].getPath();
        Reader reader = HFile.createReader(fs, dataFilePath, new CacheConfig(conf), true, conf);
        Map<byte[], byte[]> fileInfo = reader.getHFileInfo();

        byte[] bloomFilter = fileInfo.get(BLOOM_FILTER_TYPE_KEY);
        if (bloomFilter == null) bloomFilter = Bytes.toBytes("NONE");
        assertEquals("Incorrect bloom filter used for column family " + familyStr +
          "(reader: " + reader + ")",
          hcd.getBloomFilterType(), BloomType.valueOf(Bytes.toString(bloomFilter)));
        assertEquals("Incorrect compression used for column family " + familyStr +
          "(reader: " + reader + ")", hcd.getCompressionType(), reader.getFileContext().getCompression());
      }
    } finally {
      dir.getFileSystem(conf).delete(dir, true);
    }
  }

  /**
   * Write random values to the writer assuming a table created using
   * {@link #FAMILIES} as column family descriptors
   */
  private void writeRandomKeyValues(RecordWriter<ImmutableBytesWritable, Cell> writer,
      TaskAttemptContext context, Set<byte[]> families, int numRows)
      throws IOException, InterruptedException {
    byte keyBytes[] = new byte[Bytes.SIZEOF_INT];
    int valLength = 10;
    byte valBytes[] = new byte[valLength];

    int taskId = context.getTaskAttemptID().getTaskID().getId();
    assert taskId < Byte.MAX_VALUE : "Unit tests dont support > 127 tasks!";
    final byte [] qualifier = Bytes.toBytes("data");
    Random random = new Random();
    for (int i = 0; i < numRows; i++) {

      Bytes.putInt(keyBytes, 0, i);
      random.nextBytes(valBytes);
      ImmutableBytesWritable key = new ImmutableBytesWritable(keyBytes);

      for (byte[] family : families) {
        Cell kv = new KeyValue(keyBytes, family, qualifier, valBytes);
        writer.write(key, kv);
      }
    }
  }

  /**
   * This test is to test the scenario happened in HBASE-6901.
   * All files are bulk loaded and excluded from minor compaction.
   * Without the fix of HBASE-6901, an ArrayIndexOutOfBoundsException
   * will be thrown.
   */
  @Ignore ("Flakey: See HBASE-9051") @Test
  public void testExcludeAllFromMinorCompaction() throws Exception {
    Configuration conf = util.getConfiguration();
    conf.setInt("hbase.hstore.compaction.min", 2);
    generateRandomStartKeys(5);

    util.startMiniCluster();
    try (Connection conn = ConnectionFactory.createConnection();
        Admin admin = conn.getAdmin();
        Table table = util.createTable(TABLE_NAMES[0], FAMILIES);
        RegionLocator locator = conn.getRegionLocator(TABLE_NAMES[0])) {
      final FileSystem fs = util.getDFSCluster().getFileSystem();
      assertEquals("Should start with empty table", 0, util.countRows(table));

      // deep inspection: get the StoreFile dir
      final Path storePath = new Path(
        CommonFSUtils.getTableDir(CommonFSUtils.getRootDir(conf), TABLE_NAMES[0]),
          new Path(admin.getTableRegions(TABLE_NAMES[0]).get(0).getEncodedName(),
            Bytes.toString(FAMILIES[0])));
      assertEquals(0, fs.listStatus(storePath).length);

      // Generate two bulk load files
      conf.setBoolean("hbase.mapreduce.hfileoutputformat.compaction.exclude",
          true);

      for (int i = 0; i < 2; i++) {
        Path testDir = util.getDataTestDirOnTestFS("testExcludeAllFromMinorCompaction_" + i);
        runIncrementalPELoad(conf, Arrays.asList(new HFileOutputFormat2.TableInfo(table
                .getTableDescriptor(), conn.getRegionLocator(TABLE_NAMES[0]))), testDir, false);
        // Perform the actual load
        new LoadIncrementalHFiles(conf).doBulkLoad(testDir, admin, table, locator);
      }

      // Ensure data shows up
      int expectedRows = 2 * NMapInputFormat.getNumMapTasks(conf) * ROWSPERSPLIT;
      assertEquals("LoadIncrementalHFiles should put expected data in table",
          expectedRows, util.countRows(table));

      // should have a second StoreFile now
      assertEquals(2, fs.listStatus(storePath).length);

      // minor compactions shouldn't get rid of the file
      admin.compact(TABLE_NAMES[0]);
      try {
        quickPoll(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            List<HRegion> regions = util.getMiniHBaseCluster().getRegions(TABLE_NAMES[0]);
            for (HRegion region : regions) {
              for (HStore store : region.getStores()) {
                store.closeAndArchiveCompactedFiles();
              }
            }
            return fs.listStatus(storePath).length == 1;
          }
        }, 5000);
        throw new IOException("SF# = " + fs.listStatus(storePath).length);
      } catch (AssertionError ae) {
        // this is expected behavior
      }

      // a major compaction should work though
      admin.majorCompact(TABLE_NAMES[0]);
      quickPoll(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          List<HRegion> regions = util.getMiniHBaseCluster().getRegions(TABLE_NAMES[0]);
          for (HRegion region : regions) {
            for (HStore store : region.getStores()) {
              store.closeAndArchiveCompactedFiles();
            }
          }
          return fs.listStatus(storePath).length == 1;
        }
      }, 5000);

    } finally {
      util.shutdownMiniCluster();
    }
  }

  @Ignore("Goes zombie too frequently; needs work. See HBASE-14563") @Test
  public void testExcludeMinorCompaction() throws Exception {
    Configuration conf = util.getConfiguration();
    conf.setInt("hbase.hstore.compaction.min", 2);
    generateRandomStartKeys(5);

    util.startMiniCluster();
    try (Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin()){
      Path testDir = util.getDataTestDirOnTestFS("testExcludeMinorCompaction");
      final FileSystem fs = util.getDFSCluster().getFileSystem();
      Table table = util.createTable(TABLE_NAMES[0], FAMILIES);
      assertEquals("Should start with empty table", 0, util.countRows(table));

      // deep inspection: get the StoreFile dir
      final Path storePath = new Path(
        CommonFSUtils.getTableDir(CommonFSUtils.getRootDir(conf), TABLE_NAMES[0]),
          new Path(admin.getTableRegions(TABLE_NAMES[0]).get(0).getEncodedName(),
            Bytes.toString(FAMILIES[0])));
      assertEquals(0, fs.listStatus(storePath).length);

      // put some data in it and flush to create a storefile
      Put p = new Put(Bytes.toBytes("test"));
      p.addColumn(FAMILIES[0], Bytes.toBytes("1"), Bytes.toBytes("1"));
      table.put(p);
      admin.flush(TABLE_NAMES[0]);
      assertEquals(1, util.countRows(table));
      quickPoll(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return fs.listStatus(storePath).length == 1;
        }
      }, 5000);

      // Generate a bulk load file with more rows
      conf.setBoolean("hbase.mapreduce.hfileoutputformat.compaction.exclude",
          true);

      RegionLocator regionLocator = conn.getRegionLocator(TABLE_NAMES[0]);
      runIncrementalPELoad(conf, Arrays.asList(new HFileOutputFormat2.TableInfo(table
                      .getTableDescriptor(), regionLocator)), testDir, false);

      // Perform the actual load
      new LoadIncrementalHFiles(conf).doBulkLoad(testDir, admin, table, regionLocator);

      // Ensure data shows up
      int expectedRows = NMapInputFormat.getNumMapTasks(conf) * ROWSPERSPLIT;
      assertEquals("LoadIncrementalHFiles should put expected data in table",
          expectedRows + 1, util.countRows(table));

      // should have a second StoreFile now
      assertEquals(2, fs.listStatus(storePath).length);

      // minor compactions shouldn't get rid of the file
      admin.compact(TABLE_NAMES[0]);
      try {
        quickPoll(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            return fs.listStatus(storePath).length == 1;
          }
        }, 5000);
        throw new IOException("SF# = " + fs.listStatus(storePath).length);
      } catch (AssertionError ae) {
        // this is expected behavior
      }

      // a major compaction should work though
      admin.majorCompact(TABLE_NAMES[0]);
      quickPoll(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return fs.listStatus(storePath).length == 1;
        }
      }, 5000);

    } finally {
      util.shutdownMiniCluster();
    }
  }

  private void quickPoll(Callable<Boolean> c, int waitMs) throws Exception {
    int sleepMs = 10;
    int retries = (int) Math.ceil(((double) waitMs) / sleepMs);
    while (retries-- > 0) {
      if (c.call().booleanValue()) {
        return;
      }
      Thread.sleep(sleepMs);
    }
    fail();
  }

  public static void main(String args[]) throws Exception {
    new TestCellBasedHFileOutputFormat2().manualTest(args);
  }

  public void manualTest(String args[]) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    util = new HBaseTestingUtility(conf);
    if ("newtable".equals(args[0])) {
      TableName tname = TableName.valueOf(args[1]);
      byte[][] splitKeys = generateRandomSplitKeys(4);
      Table table = util.createTable(tname, FAMILIES, splitKeys);
    } else if ("incremental".equals(args[0])) {
      TableName tname = TableName.valueOf(args[1]);
      try(Connection c = ConnectionFactory.createConnection(conf);
          Admin admin = c.getAdmin();
          RegionLocator regionLocator = c.getRegionLocator(tname)) {
        Path outDir = new Path("incremental-out");
        runIncrementalPELoad(conf, Arrays.asList(new HFileOutputFormat2.TableInfo(admin
                .getTableDescriptor(tname), regionLocator)), outDir, false);
      }
    } else {
      throw new RuntimeException(
          "usage: TestHFileOutputFormat2 newtable | incremental");
    }
  }

  @Test
  public void testBlockStoragePolicy() throws Exception {
    util = new HBaseTestingUtility();
    Configuration conf = util.getConfiguration();
    conf.set(HFileOutputFormat2.STORAGE_POLICY_PROPERTY, "ALL_SSD");

    conf.set(HFileOutputFormat2.STORAGE_POLICY_PROPERTY_CF_PREFIX +
            Bytes.toString(HFileOutputFormat2.combineTableNameSuffix(
                    TABLE_NAMES[0].getName(), FAMILIES[0])), "ONE_SSD");
    Path cf1Dir = new Path(util.getDataTestDir(), Bytes.toString(FAMILIES[0]));
    Path cf2Dir = new Path(util.getDataTestDir(), Bytes.toString(FAMILIES[1]));
    util.startMiniDFSCluster(3);
    FileSystem fs = util.getDFSCluster().getFileSystem();
    try {
      fs.mkdirs(cf1Dir);
      fs.mkdirs(cf2Dir);

      // the original block storage policy would be HOT
      String spA = getStoragePolicyName(fs, cf1Dir);
      String spB = getStoragePolicyName(fs, cf2Dir);
      LOG.debug("Storage policy of cf 0: [" + spA + "].");
      LOG.debug("Storage policy of cf 1: [" + spB + "].");
      assertEquals("HOT", spA);
      assertEquals("HOT", spB);

      // alter table cf schema to change storage policies
      HFileOutputFormat2.configureStoragePolicy(conf, fs,
              HFileOutputFormat2.combineTableNameSuffix(TABLE_NAMES[0].getName(), FAMILIES[0]), cf1Dir);
      HFileOutputFormat2.configureStoragePolicy(conf, fs,
              HFileOutputFormat2.combineTableNameSuffix(TABLE_NAMES[0].getName(), FAMILIES[1]), cf2Dir);
      spA = getStoragePolicyName(fs, cf1Dir);
      spB = getStoragePolicyName(fs, cf2Dir);
      LOG.debug("Storage policy of cf 0: [" + spA + "].");
      LOG.debug("Storage policy of cf 1: [" + spB + "].");
      assertNotNull(spA);
      assertEquals("ONE_SSD", spA);
      assertNotNull(spB);
      assertEquals("ALL_SSD", spB);
    } finally {
      fs.delete(cf1Dir, true);
      fs.delete(cf2Dir, true);
      util.shutdownMiniDFSCluster();
    }
  }

  private String getStoragePolicyName(FileSystem fs, Path path) {
    try {
      Object blockStoragePolicySpi = ReflectionUtils.invokeMethod(fs, "getStoragePolicy", path);
      return (String) ReflectionUtils.invokeMethod(blockStoragePolicySpi, "getName");
    } catch (Exception e) {
      // Maybe fail because of using old HDFS version, try the old way
      if (LOG.isTraceEnabled()) {
        LOG.trace("Failed to get policy directly", e);
      }
      String policy = getStoragePolicyNameForOldHDFSVersion(fs, path);
      return policy == null ? "HOT" : policy;// HOT by default
    }
  }

  private String getStoragePolicyNameForOldHDFSVersion(FileSystem fs, Path path) {
    try {
      if (fs instanceof DistributedFileSystem) {
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        HdfsFileStatus status = dfs.getClient().getFileInfo(path.toUri().getPath());
        if (null != status) {
          byte storagePolicyId = status.getStoragePolicy();
          Field idUnspecified = BlockStoragePolicySuite.class.getField("ID_UNSPECIFIED");
          if (storagePolicyId != idUnspecified.getByte(BlockStoragePolicySuite.class)) {
            BlockStoragePolicy[] policies = dfs.getStoragePolicies();
            for (BlockStoragePolicy policy : policies) {
              if (policy.getId() == storagePolicyId) {
                return policy.getName();
              }
            }
          }
        }
      }
    } catch (Throwable e) {
      LOG.warn("failed to get block storage policy of [" + path + "]", e);
    }

    return null;
  }
}

