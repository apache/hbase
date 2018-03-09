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
package org.apache.hadoop.hbase.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.IntegrationTestBase;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IntegrationTestBulkLoad;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.Partitioner;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * Test Bulk Load and Spark on a distributed cluster.
 * It starts an Spark job that creates linked chains.
 * This test mimic {@link IntegrationTestBulkLoad} in mapreduce.
 *
 * Usage on cluster:
 *   First add hbase related jars and hbase-spark.jar into spark classpath.
 *
 *   spark-submit --class org.apache.hadoop.hbase.spark.IntegrationTestSparkBulkLoad
 *                HBASE_HOME/lib/hbase-spark-it-XXX-tests.jar -m slowDeterministic
 *                -Dhbase.spark.bulkload.chainlength=300
 */
public class IntegrationTestSparkBulkLoad extends IntegrationTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestSparkBulkLoad.class);

  // The number of partitions for random generated data
  private static String BULKLOAD_PARTITIONS_NUM = "hbase.spark.bulkload.partitionsnum";
  private static int DEFAULT_BULKLOAD_PARTITIONS_NUM = 3;

  private static String BULKLOAD_CHAIN_LENGTH = "hbase.spark.bulkload.chainlength";
  private static int DEFAULT_BULKLOAD_CHAIN_LENGTH = 200000;

  private static String BULKLOAD_IMPORT_ROUNDS = "hbase.spark.bulkload.importround";
  private static int DEFAULT_BULKLOAD_IMPORT_ROUNDS  = 1;

  private static String CURRENT_ROUND_NUM = "hbase.spark.bulkload.current.roundnum";

  private static String NUM_REPLICA_COUNT_KEY = "hbase.spark.bulkload.replica.countkey";
  private static int DEFAULT_NUM_REPLICA_COUNT = 1;

  private static String BULKLOAD_TABLE_NAME = "hbase.spark.bulkload.tableName";
  private static String DEFAULT_BULKLOAD_TABLE_NAME = "IntegrationTestSparkBulkLoad";

  private static String BULKLOAD_OUTPUT_PATH = "hbase.spark.bulkload.output.path";

  private static final String OPT_LOAD = "load";
  private static final String OPT_CHECK = "check";

  private boolean load = false;
  private boolean check = false;

  private static final byte[] CHAIN_FAM  = Bytes.toBytes("L");
  private static final byte[] SORT_FAM = Bytes.toBytes("S");
  private static final byte[] DATA_FAM = Bytes.toBytes("D");

  /**
   * Running spark job to load data into hbase table
   */
  public void runLoad() throws Exception {
    setupTable();
    int numImportRounds = getConf().getInt(BULKLOAD_IMPORT_ROUNDS, DEFAULT_BULKLOAD_IMPORT_ROUNDS);
    LOG.info("Running load with numIterations:" + numImportRounds);
    for (int i = 0; i < numImportRounds; i++) {
      runLinkedListSparkJob(i);
    }
  }

  /**
   * Running spark job to create LinkedList for testing
   * @param iteration iteration th of this job
   * @throws Exception if an HBase operation or getting the test directory fails
   */
  public void runLinkedListSparkJob(int iteration) throws Exception {
    String jobName =  IntegrationTestSparkBulkLoad.class.getSimpleName() + " _load " +
        EnvironmentEdgeManager.currentTime();

    LOG.info("Running iteration " + iteration + "in Spark Job");

    Path output = null;
    if (conf.get(BULKLOAD_OUTPUT_PATH) == null) {
      output = util.getDataTestDirOnTestFS(getTablename() + "-" + iteration);
    } else {
      output = new Path(conf.get(BULKLOAD_OUTPUT_PATH));
    }

    SparkConf sparkConf = new SparkConf().setAppName(jobName).setMaster("local");
    Configuration hbaseConf = new Configuration(getConf());
    hbaseConf.setInt(CURRENT_ROUND_NUM, iteration);
    int partitionNum = hbaseConf.getInt(BULKLOAD_PARTITIONS_NUM, DEFAULT_BULKLOAD_PARTITIONS_NUM);


    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, hbaseConf);


    LOG.info("Partition RDD into " + partitionNum + " parts");
    List<String> temp = new ArrayList<>();
    JavaRDD<List<byte[]>> rdd = jsc.parallelize(temp, partitionNum).
        mapPartitionsWithIndex(new LinkedListCreationMapper(new SerializableWritable<>(hbaseConf)),
                false);

    hbaseContext.bulkLoad(rdd, getTablename(), new ListToKeyValueFunc(), output.toUri().getPath(),
        new HashMap<>(), false, HConstants.DEFAULT_MAX_FILE_SIZE);

    try (Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        Table table = conn.getTable(getTablename());
        RegionLocator regionLocator = conn.getRegionLocator(getTablename())) {
      // Create a new loader.
      LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);

      // Load the HFiles into table.
      loader.doBulkLoad(output, admin, table, regionLocator);
    }


    // Delete the files.
    util.getTestFileSystem().delete(output, true);
    jsc.close();
  }

  // See mapreduce.IntegrationTestBulkLoad#LinkedListCreationMapper
  // Used to generate test data
  public static class LinkedListCreationMapper implements
      Function2<Integer, Iterator<String>, Iterator<List<byte[]>>> {

    SerializableWritable swConfig = null;
    private Random rand = new Random();

    public LinkedListCreationMapper(SerializableWritable conf) {
      this.swConfig = conf;
    }

    @Override
    public Iterator<List<byte[]>> call(Integer v1, Iterator v2) throws Exception {
      Configuration config = (Configuration) swConfig.value();
      int partitionId = v1.intValue();
      LOG.info("Starting create List in Partition " + partitionId);

      int partitionNum = config.getInt(BULKLOAD_PARTITIONS_NUM, DEFAULT_BULKLOAD_PARTITIONS_NUM);
      int chainLength = config.getInt(BULKLOAD_CHAIN_LENGTH, DEFAULT_BULKLOAD_CHAIN_LENGTH);
      int iterationsNum = config.getInt(BULKLOAD_IMPORT_ROUNDS, DEFAULT_BULKLOAD_IMPORT_ROUNDS);
      int iterationsCur = config.getInt(CURRENT_ROUND_NUM, 0);
      List<List<byte[]>> res = new LinkedList<>();


      long tempId = partitionId + iterationsCur * partitionNum;
      long totalPartitionNum = partitionNum * iterationsNum;
      long chainId = Math.abs(rand.nextLong());
      chainId = chainId - (chainId % totalPartitionNum) + tempId;

      byte[] chainIdArray = Bytes.toBytes(chainId);
      long currentRow = 0;
      long nextRow = getNextRow(0, chainLength);
      for(long i = 0; i < chainLength; i++) {
        byte[] rk = Bytes.toBytes(currentRow);
        // Insert record into a list
        List<byte[]> tmp1 = Arrays.asList(rk, CHAIN_FAM, chainIdArray, Bytes.toBytes(nextRow));
        List<byte[]> tmp2 = Arrays.asList(rk, SORT_FAM, chainIdArray, Bytes.toBytes(i));
        List<byte[]> tmp3 = Arrays.asList(rk, DATA_FAM, chainIdArray, Bytes.toBytes(
            RandomStringUtils.randomAlphabetic(50)));
        res.add(tmp1);
        res.add(tmp2);
        res.add(tmp3);

        currentRow = nextRow;
        nextRow = getNextRow(i+1, chainLength);
      }
      return res.iterator();
    }

    /** Returns a unique row id within this chain for this index */
    private long getNextRow(long index, long chainLength) {
      long nextRow = Math.abs(new Random().nextLong());
      // use significant bits from the random number, but pad with index to ensure it is unique
      // this also ensures that we do not reuse row = 0
      // row collisions from multiple mappers are fine, since we guarantee unique chainIds
      nextRow = nextRow - (nextRow % chainLength) + index;
      return nextRow;
    }
  }



  public static class ListToKeyValueFunc implements
      Function<List<byte[]>, Pair<KeyFamilyQualifier, byte[]>> {
    @Override
    public Pair<KeyFamilyQualifier, byte[]> call(List<byte[]> v1) throws Exception {
      if (v1 == null || v1.size() != 4) {
        return null;
      }
      KeyFamilyQualifier kfq = new KeyFamilyQualifier(v1.get(0), v1.get(1), v1.get(2));

      return new Pair<>(kfq, v1.get(3));
    }
  }

  /**
   * After adding data to the table start a mr job to check the bulk load.
   */
  public void runCheck() throws Exception {
    LOG.info("Running check");
    String jobName = IntegrationTestSparkBulkLoad.class.getSimpleName() + "_check" +
            EnvironmentEdgeManager.currentTime();

    SparkConf sparkConf = new SparkConf().setAppName(jobName).setMaster("local");
    Configuration hbaseConf = new Configuration(getConf());
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, hbaseConf);

    Scan scan = new Scan();
    scan.addFamily(CHAIN_FAM);
    scan.addFamily(SORT_FAM);
    scan.setMaxVersions(1);
    scan.setCacheBlocks(false);
    scan.setBatch(1000);
    int replicaCount = conf.getInt(NUM_REPLICA_COUNT_KEY, DEFAULT_NUM_REPLICA_COUNT);
    if (replicaCount != DEFAULT_NUM_REPLICA_COUNT) {
      scan.setConsistency(Consistency.TIMELINE);
    }

    // 1. Using TableInputFormat to get data from HBase table
    // 2. Mimic LinkedListCheckingMapper in mapreduce.IntegrationTestBulkLoad
    // 3. Sort LinkKey by its order ID
    // 4. Group LinkKey if they have same chainId, and repartition RDD by NaturalKeyPartitioner
    // 5. Check LinkList in each Partition using LinkedListCheckingFlatMapFunc
    hbaseContext.hbaseRDD(getTablename(), scan).flatMapToPair(new LinkedListCheckingFlatMapFunc())
        .sortByKey()
        .combineByKey(new createCombinerFunc(), new mergeValueFunc(), new mergeCombinersFunc(),
            new NaturalKeyPartitioner(new SerializableWritable<>(hbaseConf)))
        .foreach(new LinkedListCheckingForeachFunc(new SerializableWritable<>(hbaseConf)));
    jsc.close();
  }

  private void runCheckWithRetry() throws Exception {
    try {
      runCheck();
    } catch (Throwable t) {
      LOG.warn("Received " + StringUtils.stringifyException(t));
      LOG.warn("Running the check MR Job again to see whether an ephemeral problem or not");
      runCheck();
      throw t; // we should still fail the test even if second retry succeeds
    }
    // everything green
  }

  /**
   * PairFlatMapFunction used to transfer {@code <Row, Result>} to
   * {@code Tuple<SparkLinkKey, SparkLinkChain>}.
   */
  public static class LinkedListCheckingFlatMapFunc implements
      PairFlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, SparkLinkKey, SparkLinkChain> {

    @Override
    public Iterator<Tuple2<SparkLinkKey, SparkLinkChain>> call(Tuple2<ImmutableBytesWritable,
            Result> v) throws Exception {
      Result value = v._2();
      long longRk = Bytes.toLong(value.getRow());
      List<Tuple2<SparkLinkKey, SparkLinkChain>> list = new LinkedList<>();

      for (Map.Entry<byte[], byte[]> entry : value.getFamilyMap(CHAIN_FAM).entrySet()) {
        long chainId = Bytes.toLong(entry.getKey());
        long next = Bytes.toLong(entry.getValue());
        Cell c = value.getColumnCells(SORT_FAM, entry.getKey()).get(0);
        long order = Bytes.toLong(CellUtil.cloneValue(c));
        Tuple2<SparkLinkKey, SparkLinkChain> tuple2 =
            new Tuple2<>(new SparkLinkKey(chainId, order), new SparkLinkChain(longRk, next));
        list.add(tuple2);
      }
      return list.iterator();
    }
  }

  public static class createCombinerFunc implements
      Function<SparkLinkChain, List<SparkLinkChain>> {
    @Override
    public List<SparkLinkChain> call(SparkLinkChain v1) throws Exception {
      List<SparkLinkChain> list = new LinkedList<>();
      list.add(v1);
      return list;
    }
  }

  public static class mergeValueFunc implements
      Function2<List<SparkLinkChain>, SparkLinkChain, List<SparkLinkChain>> {
    @Override
    public List<SparkLinkChain> call(List<SparkLinkChain> v1, SparkLinkChain v2) throws Exception {
      if (v1 == null) {
        v1 = new LinkedList<>();
      }

      v1.add(v2);
      return v1;
    }
  }

  public static class mergeCombinersFunc implements
      Function2<List<SparkLinkChain>, List<SparkLinkChain>, List<SparkLinkChain>> {
    @Override
    public List<SparkLinkChain> call(List<SparkLinkChain> v1, List<SparkLinkChain> v2)
            throws Exception {
      v1.addAll(v2);
      return v1;
    }
  }

  /**
   * Class to figure out what partition to send a link in the chain to.  This is based upon
   * the linkKey's ChainId.
   */
  public static class NaturalKeyPartitioner extends Partitioner {

    private int numPartions = 0;
    public NaturalKeyPartitioner(SerializableWritable swConf) {
      Configuration hbaseConf = (Configuration) swConf.value();
      numPartions = hbaseConf.getInt(BULKLOAD_PARTITIONS_NUM, DEFAULT_BULKLOAD_PARTITIONS_NUM);

    }

    @Override
    public int numPartitions() {
      return numPartions;
    }

    @Override
    public int getPartition(Object key) {
      if (!(key instanceof SparkLinkKey)) {
        return -1;
      }

      int hash = ((SparkLinkKey) key).getChainId().hashCode();
      return Math.abs(hash % numPartions);

    }
  }

  /**
   * Sort all LinkChain for one LinkKey, and test {@code List<LinkChain>}.
   */
  public static class LinkedListCheckingForeachFunc
      implements VoidFunction<Tuple2<SparkLinkKey, List<SparkLinkChain>>> {

    private  SerializableWritable swConf = null;

    public LinkedListCheckingForeachFunc(SerializableWritable conf) {
      swConf = conf;
    }

    @Override
    public void call(Tuple2<SparkLinkKey, List<SparkLinkChain>> v1) throws Exception {
      long next = -1L;
      long prev = -1L;
      long count = 0L;

      SparkLinkKey key = v1._1();
      List<SparkLinkChain> values = v1._2();

      for (SparkLinkChain lc : values) {

        if (next == -1) {
          if (lc.getRk() != 0L) {
            String msg = "Chains should all start at rk 0, but read rk " + lc.getRk()
                + ". Chain:" + key.getChainId() + ", order:" + key.getOrder();
            throw new RuntimeException(msg);
          }
          next = lc.getNext();
        } else {
          if (next != lc.getRk()) {
            String msg = "Missing a link in the chain. Prev rk " + prev + " was, expecting "
                + next + " but got " + lc.getRk() + ". Chain:" + key.getChainId()
                + ", order:" + key.getOrder();
            throw new RuntimeException(msg);
          }
          prev = lc.getRk();
          next = lc.getNext();
        }
        count++;
      }
      Configuration hbaseConf = (Configuration) swConf.value();
      int expectedChainLen = hbaseConf.getInt(BULKLOAD_CHAIN_LENGTH, DEFAULT_BULKLOAD_CHAIN_LENGTH);
      if (count != expectedChainLen) {
        String msg = "Chain wasn't the correct length.  Expected " + expectedChainLen + " got "
            + count + ". Chain:" + key.getChainId() + ", order:" + key.getOrder();
        throw new RuntimeException(msg);
      }
    }
  }

  /**
   * Writable class used as the key to group links in the linked list.
   *
   * Used as the key emited from a pass over the table.
   */
  public static class SparkLinkKey implements java.io.Serializable, Comparable<SparkLinkKey> {

    private Long chainId;
    private Long order;

    public Long getOrder() {
      return order;
    }

    public Long getChainId() {
      return chainId;
    }

    public SparkLinkKey(long chainId, long order) {
      this.chainId = chainId;
      this.order = order;
    }

    @Override
    public int hashCode() {
      return this.getChainId().hashCode();
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof SparkLinkKey)) {
        return false;
      }

      SparkLinkKey otherKey = (SparkLinkKey) other;
      return this.getChainId().equals(otherKey.getChainId());
    }

    @Override
    public int compareTo(SparkLinkKey other) {
      int res = getChainId().compareTo(other.getChainId());

      if (res == 0) {
        res = getOrder().compareTo(other.getOrder());
      }

      return res;
    }
  }

  /**
   * Writable used as the value emitted from a pass over the hbase table.
   */
  public static class SparkLinkChain implements java.io.Serializable, Comparable<SparkLinkChain>{

    public Long getNext() {
      return next;
    }

    public Long getRk() {
      return rk;
    }


    public SparkLinkChain(Long rk, Long next) {
      this.rk = rk;
      this.next = next;
    }

    private Long rk;
    private Long next;

    @Override
    public int compareTo(SparkLinkChain linkChain) {
      int res = getRk().compareTo(linkChain.getRk());
      if (res == 0) {
        res = getNext().compareTo(linkChain.getNext());
      }
      return res;
    }

    @Override
    public int hashCode() {
      return getRk().hashCode() ^ getNext().hashCode();
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof SparkLinkChain)) {
        return false;
      }

      SparkLinkChain otherKey = (SparkLinkChain) other;
      return this.getRk().equals(otherKey.getRk()) && this.getNext().equals(otherKey.getNext());
    }
  }


  /**
   * Allow the scan to go to replica, this would not affect the runCheck()
   * Since data are BulkLoaded from HFile into table
   * @throws IOException if an HBase operation fails
   * @throws InterruptedException if modifying the table fails
   */
  private void installSlowingCoproc() throws IOException, InterruptedException {
    int replicaCount = conf.getInt(NUM_REPLICA_COUNT_KEY, DEFAULT_NUM_REPLICA_COUNT);

    if (replicaCount == DEFAULT_NUM_REPLICA_COUNT) {
      return;
    }

    TableName t = getTablename();
    Admin admin = util.getAdmin();
    HTableDescriptor desc = admin.getTableDescriptor(t);
    desc.addCoprocessor(IntegrationTestBulkLoad.SlowMeCoproScanOperations.class.getName());
    HBaseTestingUtility.modifyTableSync(admin, desc);
  }

  @Test
  public void testBulkLoad() throws Exception {
    runLoad();
    installSlowingCoproc();
    runCheckWithRetry();
  }


  private byte[][] getSplits(int numRegions) {
    RegionSplitter.UniformSplit split = new RegionSplitter.UniformSplit();
    split.setFirstRow(Bytes.toBytes(0L));
    split.setLastRow(Bytes.toBytes(Long.MAX_VALUE));
    return split.split(numRegions);
  }

  private void setupTable() throws IOException, InterruptedException {
    if (util.getAdmin().tableExists(getTablename())) {
      util.deleteTable(getTablename());
    }

    util.createTable(
        getTablename(),
        new byte[][]{CHAIN_FAM, SORT_FAM, DATA_FAM},
        getSplits(16)
    );

    int replicaCount = conf.getInt(NUM_REPLICA_COUNT_KEY, DEFAULT_NUM_REPLICA_COUNT);

    if (replicaCount == DEFAULT_NUM_REPLICA_COUNT) {
      return;
    }

    TableName t = getTablename();
    HBaseTestingUtility.setReplicas(util.getAdmin(), t, replicaCount);
  }

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(getConf());
    util.initializeCluster(1);
    int replicaCount = getConf().getInt(NUM_REPLICA_COUNT_KEY, DEFAULT_NUM_REPLICA_COUNT);
    if (LOG.isDebugEnabled() && replicaCount != DEFAULT_NUM_REPLICA_COUNT) {
      LOG.debug("Region Replicas enabled: " + replicaCount);
    }

    // Scale this up on a real cluster
    if (util.isDistributedCluster()) {
      util.getConfiguration().setIfUnset(BULKLOAD_PARTITIONS_NUM,
              String.valueOf(DEFAULT_BULKLOAD_PARTITIONS_NUM));
      util.getConfiguration().setIfUnset(BULKLOAD_IMPORT_ROUNDS, "1");
    } else {
      util.startMiniMapReduceCluster();
    }
  }

  @Override
  protected void addOptions() {
    super.addOptions();
    super.addOptNoArg(OPT_CHECK, "Run check only");
    super.addOptNoArg(OPT_LOAD, "Run load only");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    super.processOptions(cmd);
    check = cmd.hasOption(OPT_CHECK);
    load = cmd.hasOption(OPT_LOAD);
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    if (load) {
      runLoad();
    } else if (check) {
      installSlowingCoproc();
      runCheckWithRetry();
    } else {
      testBulkLoad();
    }
    return 0;
  }

  @Override
  public TableName getTablename() {
    return getTableName(getConf());
  }

  public static TableName getTableName(Configuration conf) {
    return TableName.valueOf(conf.get(BULKLOAD_TABLE_NAME, DEFAULT_BULKLOAD_TABLE_NAME));
  }

  @Override
  protected Set<String> getColumnFamilies() {
    return Sets.newHashSet(Bytes.toString(CHAIN_FAM) , Bytes.toString(DATA_FAM),
        Bytes.toString(SORT_FAM));
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int status =  ToolRunner.run(conf, new IntegrationTestSparkBulkLoad(), args);
    System.exit(status);
  }
}
