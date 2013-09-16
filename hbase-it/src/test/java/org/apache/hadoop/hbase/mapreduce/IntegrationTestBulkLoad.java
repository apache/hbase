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

import static org.junit.Assert.assertEquals;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.IntegrationTestBase;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.IntegrationTests;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test Bulk Load and MR on a distributed cluster.
 * It starts an MR job that creates linked chains
 *
 * The format of rows is like this:
 * Row Key -> Long
 *
 * L:<< Chain Id >> -> Row Key of the next link in the chain
 * S:<< Chain Id >> -> The step in the chain that his link is.
 * D:<< Chain Id >> -> Random Data.
 *
 * All chains start on row 0.
 * All rk's are > 0.
 *
 * After creating the linked lists they are walked over using a TableMapper based Mapreduce Job.
 *
 * There are a few options exposed:
 *
 * hbase.IntegrationTestBulkLoad.chainLength
 * The number of rows that will be part of each and every chain.
 *
 * hbase.IntegrationTestBulkLoad.numMaps
 * The number of mappers that will be run.  Each mapper creates on linked list chain.
 *
 * hbase.IntegrationTestBulkLoad.numImportRounds
 * How many jobs will be run to create linked lists.
 *
 * hbase.IntegrationTestBulkLoad.tableName
 * The name of the table.
 *
 */
@Category(IntegrationTests.class)
public class IntegrationTestBulkLoad extends IntegrationTestBase {

  private static byte[] CHAIN_FAM = Bytes.toBytes("L");
  private static byte[] SORT_FAM  = Bytes.toBytes("S");
  private static byte[] DATA_FAM  = Bytes.toBytes("D");

  private static String CHAIN_LENGTH_KEY = "hbase.IntegrationTestBulkLoad.chainLength";
  private static int CHAIN_LENGTH = 500000;

  private static String NUM_MAPS_KEY = "hbase.IntegrationTestBulkLoad.numMaps";
  private static int NUM_MAPS = 1;

  private static String NUM_IMPORT_ROUNDS_KEY = "hbase.IntegrationTestBulkLoad.numImportRounds";
  private static int NUM_IMPORT_ROUNDS = 1;


  private static String TABLE_NAME_KEY = "hbase.IntegrationTestBulkLoad.tableName";
  private static String TABLE_NAME = "IntegrationTestBulkLoad";

  @Test
  public void testBulkLoad() throws Exception {
    setupTable();
    int numImportRounds = getConf().getInt(NUM_IMPORT_ROUNDS_KEY, NUM_IMPORT_ROUNDS);
    for (int i = 0; i < numImportRounds; i++) {
      runLinkedListMRJob(i);
    }
    runCheck();
  }

  private byte[][] getSplits(int numRegions) {
    RegionSplitter.UniformSplit split = new RegionSplitter.UniformSplit();
    split.setFirstRow(Bytes.toBytes(0L));
    split.setLastRow(Bytes.toBytes(Long.MAX_VALUE));
    return split.split(numRegions);
  }

  private void setupTable() throws IOException {
    if (util.getHBaseAdmin().tableExists(getTablename())) {
      util.deleteTable(getTablename());
    }

    util.createTable(
        Bytes.toBytes(getTablename()),
        new byte[][]{CHAIN_FAM, SORT_FAM, DATA_FAM},
        getSplits(16)
    );
  }

  private void runLinkedListMRJob(int iteration) throws Exception {
    String jobName =  IntegrationTestBulkLoad.class.getSimpleName() + " - " +
        EnvironmentEdgeManager.currentTimeMillis();
    Configuration conf = new Configuration(util.getConfiguration());
    Path p = util.getDataTestDirOnTestFS(getTablename() +  "-" + iteration);
    HTable table = new HTable(conf, getTablename());

    conf.setBoolean("mapreduce.map.speculative", false);
    conf.setBoolean("mapreduce.reduce.speculative", false);

    Job job = new Job(conf);

    job.setJobName(jobName);

    // set the input format so that we can create map tasks with no data input.
    job.setInputFormatClass(RandomInputFormat.class);

    // Set the mapper classes.
    job.setMapperClass(LinkedListCreationMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(KeyValue.class);

    // Use the identity reducer
    // So nothing to do here.

    // Set this jar.
    job.setJarByClass(getClass());

    // Set where to place the hfiles.
    FileOutputFormat.setOutputPath(job, p);

    // Configure the partitioner and other things needed for HFileOutputFormat.
    HFileOutputFormat.configureIncrementalLoad(job, table);

    // Run the job making sure it works.
    assertEquals(true, job.waitForCompletion(true));

    // Create a new loader.
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);

    // Load the HFiles in.
    loader.doBulkLoad(p, table);

    // Delete the files.
    util.getTestFileSystem().delete(p, true);
  }

  /**
   * Class to generate splits.  Each split gets a dummy split file.  The associated
   * RecordReader generates a single random number.
   *
   * This class is adapted from Hadoop tests.
   */
  static class RandomInputFormat extends InputFormat<Text, LongWritable> {
    public List<InputSplit> getSplits(JobContext job) throws IOException {
      List<InputSplit> result = new ArrayList<InputSplit>();
      int numSplits = job.getConfiguration().getInt(NUM_MAPS_KEY, NUM_MAPS);
      for (int i = 0; i < numSplits; ++i) {
        result.add(new FileSplit(new Path("/tmp", "dummy-split-" + i), 0, 1, null));
      }
      return result;
    }

    /**
     * RecordReader that doesn't read anything.  Instead it generates a single random number.
     * This is useful for debugging or starting map tasks with no data inpput.
     *
     * This class is adapted from Hadoop tests.
     */
    static class RandomRecordReader extends RecordReader<Text, LongWritable> {
      Path name;
      Text key = null;
      LongWritable value = new LongWritable();

      public RandomRecordReader(Path p) {
        name = p;
      }

      public void initialize(InputSplit split,
                             TaskAttemptContext context)
          throws IOException, InterruptedException {

      }

      public boolean nextKeyValue() {
        if (name != null) {
          key = new Text();
          key.set(name.getName());
          name = null;
          value.set(new Random().nextLong());
          return true;
        }
        return false;
      }

      public Text getCurrentKey() {
        return key;
      }

      public LongWritable getCurrentValue() {
        return value;
      }

      public void close() {
      }

      public float getProgress() {
        return 0.0f;
      }
    }

    public RecordReader<Text, LongWritable> createRecordReader(InputSplit split,
                                                               TaskAttemptContext context)
        throws IOException, InterruptedException {
      return new RandomRecordReader(((FileSplit) split).getPath());
    }
  }

  /**
   * Mapper that creates a linked list of KeyValues.
   *
   * Each map task generates one linked list.
   * All lists start on row key 0L.
   * All lists should be CHAIN_LENGTH long.
   */
  public static class LinkedListCreationMapper
      extends Mapper<Text, LongWritable, ImmutableBytesWritable, KeyValue> {

    private Random rand = new Random();

    protected void map(Text key, LongWritable value, Context context)
        throws IOException, InterruptedException {

      long chainId = value.get();
      byte[] chainIdArray = Bytes.toBytes(chainId);
      long currentRow = 0;
      long nextRow = Math.abs(rand.nextLong());

      int chainLength = context.getConfiguration().getInt(CHAIN_LENGTH_KEY, CHAIN_LENGTH);

      for (long i = 0; i < chainLength; i++) {
        byte[] rk = Bytes.toBytes(currentRow);

        // Next link in the chain.
        KeyValue linkKv = new KeyValue(rk, CHAIN_FAM, chainIdArray, Bytes.toBytes(nextRow));
        // What link in the chain this is.
        KeyValue sortKv = new KeyValue(rk, SORT_FAM, chainIdArray, Bytes.toBytes(i));
        // Added data so that large stores are created.
        KeyValue dataKv = new KeyValue(rk, DATA_FAM, chainIdArray,
          Bytes.toBytes(RandomStringUtils.randomAlphabetic(50))
        );

        // Emit the key values.
        context.write(new ImmutableBytesWritable(rk), linkKv);
        context.write(new ImmutableBytesWritable(rk), sortKv);
        context.write(new ImmutableBytesWritable(rk), dataKv);
        // Move to the next row.
        currentRow = nextRow;
        nextRow = Math.abs(rand.nextLong());
      }
    }
  }

  /**
   * Writable class used as the key to group links in the linked list.
   *
   * Used as the key emited from a pass over the table.
   */
  public static class LinkKey implements WritableComparable<LinkKey> {

    private Long chainId;

    public Long getOrder() {
      return order;
    }

    public Long getChainId() {
      return chainId;
    }

    private Long order;

    public LinkKey() {}

    public LinkKey(long chainId, long order) {
      this.chainId = chainId;
      this.order = order;
    }

    @Override
    public int compareTo(LinkKey linkKey) {
      int res = getChainId().compareTo(linkKey.getChainId());
      if (res == 0) {
        res = getOrder().compareTo(linkKey.getOrder());
      }
      return res;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      WritableUtils.writeVLong(dataOutput, chainId);
      WritableUtils.writeVLong(dataOutput, order);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      chainId = WritableUtils.readVLong(dataInput);
      order = WritableUtils.readVLong(dataInput);
    }
  }

  /**
   * Writable used as the value emitted from a pass over the hbase table.
   */
  public static class LinkChain implements WritableComparable<LinkChain> {

    public Long getNext() {
      return next;
    }

    public Long getRk() {
      return rk;
    }

    public LinkChain() {}

    public LinkChain(Long rk, Long next) {
      this.rk = rk;
      this.next = next;
    }

    private Long rk;
    private Long next;

    @Override
    public int compareTo(LinkChain linkChain) {
      int res = getRk().compareTo(linkChain.getRk());
      if (res == 0) {
        res = getNext().compareTo(linkChain.getNext());
      }
      return res;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      WritableUtils.writeVLong(dataOutput, rk);
      WritableUtils.writeVLong(dataOutput, next);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      rk = WritableUtils.readVLong(dataInput);
      next = WritableUtils.readVLong(dataInput);
    }
  }

  /**
   * Class to figure out what partition to send a link in the chain to.  This is based upon
   * the linkKey's ChainId.
   */
  public static class NaturalKeyPartitioner extends Partitioner<LinkKey, LinkChain> {
    @Override
    public int getPartition(LinkKey linkKey,
                            LinkChain linkChain,
                            int numPartitions) {
      int hash = linkKey.getChainId().hashCode();
      return hash % numPartitions;
    }
  }

  /**
   * Comparator used to figure out if a linkKey should be grouped together.  This is based upon the
   * linkKey's ChainId.
   */
  public static class NaturalKeyGroupingComparator extends WritableComparator {

    protected NaturalKeyGroupingComparator() {
      super(LinkKey.class, true);
    }

    public int compare(WritableComparable w1, WritableComparable w2) {
      LinkKey k1 = (LinkKey) w1;
      LinkKey k2 = (LinkKey) w2;

      return k1.getChainId().compareTo(k2.getChainId());
    }
  }

  /**
   * Comparator used to order linkKeys so that they are passed to a reducer in order.  This is based
   * upon linkKey ChainId and Order.
   */
  public static class CompositeKeyComparator extends WritableComparator {

    protected CompositeKeyComparator() {
      super(LinkKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      LinkKey k1 = (LinkKey) w1;
      LinkKey k2 = (LinkKey) w2;

      return k1.compareTo(k2);
    }
  }

  /**
   * Mapper to pass over the table.
   *
   * For every row there could be multiple chains that landed on this row. So emit a linkKey
   * and value for each.
   */
  public static class LinkedListCheckingMapper extends TableMapper<LinkKey, LinkChain> {
    protected void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException, InterruptedException {
      long longRk = Bytes.toLong(value.getRow());

      for (Map.Entry<byte[], byte[]> entry : value.getFamilyMap(CHAIN_FAM).entrySet()) {
        long chainId = Bytes.toLong(entry.getKey());
        long next = Bytes.toLong(entry.getValue());
        Cell c = value.getColumnCells(SORT_FAM, entry.getKey()).get(0);
        long order = Bytes.toLong(CellUtil.cloneValue(c));
        context.write(new LinkKey(chainId, order), new LinkChain(longRk, next));
      }
    }
  }

  /**
   * Class that does the actual checking of the links.
   *
   * All links in the chain should be grouped and sorted when sent to this class.  Then the chain
   * will be traversed making sure that no link is missing and that the chain is the correct length.
   *
   * This will throw an exception if anything is not correct.  That causes the job to fail if any
   * data is corrupt.
   */
  public static class LinkedListCheckingReducer
      extends Reducer<LinkKey, LinkChain, NullWritable, NullWritable> {
    protected void reduce(LinkKey key, Iterable<LinkChain> values, Context context)
        throws java.io.IOException, java.lang.InterruptedException {
      long next = -1L;
      long count = 0L;

      for (LinkChain lc : values) {

        if (next == -1) {
          if (lc.getRk() != 0L) throw new RuntimeException("Chains should all start at 0 rk");
          next = lc.getNext();
        } else {
          if (next != lc.getRk())
            throw new RuntimeException("Missing a link in the chain. Expecthing " +
                next + " got " + lc.getRk());
          next = lc.getNext();
        }
        count++;
      }

      int expectedChainLen = context.getConfiguration().getInt(CHAIN_LENGTH_KEY, CHAIN_LENGTH);
      if (count != expectedChainLen)
        throw new RuntimeException("Chain wasn't the correct length.  Expected " +
            expectedChainLen + " got " + count);
    }
  }

  /**
   * After adding data to the table start a mr job to
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  private void runCheck() throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = getConf();
    String jobName = getTablename() + "_check" + EnvironmentEdgeManager.currentTimeMillis();
    Path p = util.getDataTestDirOnTestFS(jobName);

    Job job = new Job(conf);

    job.setJarByClass(getClass());

    job.setPartitionerClass(NaturalKeyPartitioner.class);
    job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
    job.setSortComparatorClass(CompositeKeyComparator.class);

    Scan s = new Scan();
    s.addFamily(CHAIN_FAM);
    s.addFamily(SORT_FAM);
    s.setMaxVersions(1);
    s.setCacheBlocks(false);
    s.setBatch(100);

    TableMapReduceUtil.initTableMapperJob(
        Bytes.toBytes(getTablename()),
        new Scan(),
        LinkedListCheckingMapper.class,
        LinkKey.class,
        LinkChain.class,
        job
    );

    job.setReducerClass(LinkedListCheckingReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);

    FileOutputFormat.setOutputPath(job, p);

    assertEquals(true, job.waitForCompletion(true));

    // Delete the files.
    util.getTestFileSystem().delete(p, true);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    util = getTestingUtil(getConf());
    util.initializeCluster(1);

    // Scale this up on a real cluster
    if (util.isDistributedCluster()) {
      util.getConfiguration().setIfUnset(NUM_MAPS_KEY,
          Integer.toString(util.getHBaseAdmin().getClusterStatus().getServersSize() * 10)
      );
      util.getConfiguration().setIfUnset(NUM_IMPORT_ROUNDS_KEY, "5");
    } else {
      util.startMiniMapReduceCluster();
    }
  }

  @After
  @Override
  public void cleanUp() throws Exception {
    util.restoreCluster();
    util = null;
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    runCheck();
    return 0;
  }

  @Override
  public String getTablename() {
    return getConf().get(TABLE_NAME_KEY, TABLE_NAME);
  }

  @Override
  protected Set<String> getColumnFamilies() {
    return null;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int status =  ToolRunner.run(conf, new IntegrationTestBulkLoad(), args);
    System.exit(status);
  }

}