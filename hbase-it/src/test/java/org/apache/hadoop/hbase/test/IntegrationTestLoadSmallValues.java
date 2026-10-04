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
package org.apache.hadoop.hbase.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.IntegrationTestBase;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.RandomDistribution;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This integration test emulates a use case that stores a lot of small values into a table
 * that would likely be heavily indexed (ROW_INDEX_V1, small blocks, etc.), an application that
 * crowdsources weather (temperature) observation data. This IT can be used to test and optimize
 * compression settings for such cases. It comes with a companion utility, HFileBlockExtracter,
 * which extracts block data from HFiles into a set of local files for use in training external
 * compression dictionaries, perhaps with ZStandard's 'zstd' utility.
 * <p>
 * Run like:
 * <blockquote><tt>
 * ./bin/hbase org.apache.hadoop.hbase.test.IntegrationTestLoadSmallValues<br>
 * &nbsp;&nbsp; numRows numMappers outputDir
 * </tt></blockquote>
 * <p>
 * You can also split the Loader and Verify stages:
 * <p>
 * Load with:
 * <blockquote><tt>
 * ./bin/hbase 'org.apache.hadoop.hbase.test.IntegrationTestLoadSmallValues$Loader'<br>
 * &nbsp;&nbsp; numRows numMappers outputDir
 * </tt></blockquote>
 * Verify with:
 * <blockquote><tt>
 * ./bin/hbase 'org.apache.hadoop.hbase.test.IntegrationTestLoadSmallValues$Verify'<br>
 * &nbsp;&nbsp; outputDir
 * </tt></blockquote>
 * <p>
 * Use HFileExtractor like so:
 * <blockquote><tt>
 * ./bin/hbase org.apache.hadoop.hbase.test.util.HFileExtractor<br>
 * &nbsp;&nbsp; options outputDir hfile_1 ... hfile_n
 * </tt><p>
 * Where options are:<p>
 * &nbsp;&nbsp; -d width: Width of generated file name for zero padding, default: 5 <br>
 * &nbsp;&nbsp; -n count: Total number of blocks to extract, default: unlimited <br>
 * &nbsp;&nbsp; -r | --random: Shuffle blocks and write them in randomized order
 * </blockquote>
 * <p>
 * You might train ZStandard dictionaries on the extracted block files like so:<br>
 * (Assumes outputDir given to HFileExtractor was 't'.)
 * <blockquote><tt>
 * $ zstd --train -o dict t/*<br>
 * </tt><p>
 * Or:<p>
 * <tt>
 * $ zstd --train-fastcover=k=32,d=6 -o dict t/*<br>
 * </tt></blockquote>
 */
public class IntegrationTestLoadSmallValues extends IntegrationTestBase {

  static final Logger LOG = LoggerFactory.getLogger(IntegrationTestLoadSmallValues.class);
  static final String TABLE_NAME_KEY = "IntegrationTestLoadSmallValues.table";
  static final String DEFAULT_TABLE_NAME = "IntegrationTestLoadSmallValues";
  static final byte[] INFO_FAMILY_NAME = Bytes.toBytes("i");
  static final byte[] INFO_LATITUDE = Bytes.toBytes("la");
  static final byte[] INFO_LONGITUDE = Bytes.toBytes("lo");
  static final byte[] INFO_TEMPERATURE = Bytes.toBytes("t");

  static final boolean DEFAULT_PRESPLIT_TABLE = false;
  static final int DEFAULT_BLOCK_SIZE = 2048;
  static final DataBlockEncoding DEFAULT_BLOCK_ENCODING = DataBlockEncoding.ROW_INDEX_V1;
  static final long DEFAULT_NUM_ROWS = 1_000_000;
  static final int DEFAULT_NUM_MAPS = 1;

  public static enum Counts {
    REFERENCED, UNREFERENCED, CORRUPT
  }

  protected String[] args;
  protected long numRows = DEFAULT_NUM_ROWS;
  protected int numMaps = DEFAULT_NUM_MAPS;
  protected Path outputDir = null;

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(getConf());
    final boolean isDistributed = util.isDistributedCluster();
    util.initializeCluster(isDistributed ? 1 : 3);
    if (!isDistributed) {
      util.startMiniMapReduceCluster();
    }
    this.setConf(util.getConfiguration());
  }

  @Override
  public void cleanUpCluster() throws Exception {
    super.cleanUpCluster();
    if (util.isDistributedCluster()) {
      util.shutdownMiniMapReduceCluster();
    }
  }

  @Override
  public TableName getTablename() {
    return getTableName(getConf());
  }

  @Override
  protected Set<String> getColumnFamilies() {
    Set<String> families = new HashSet<>();
    families.add(Bytes.toString(INFO_FAMILY_NAME));
    return families;
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    return ToolRunner.run(getConf(), this, args);
  }

  protected int runLoader(final long numRows, final int numMaps, final Path outputDir) {
    Loader loader = new Loader();
    loader.setConf(conf);
    try {
      return loader.run(numRows, numMaps, outputDir);
    } catch (Exception e) {
      LOG.error("Loader failed with exception", e);
      return -1;
    }
  }

  protected int runVerify(final Path inputDir) {
    Verify verify = new Verify();
    verify.setConf(conf);
    try {
      return verify.run(inputDir);
    } catch (Exception e) {
      LOG.error("Verify failed with exception", e);
      return -1;
    }
  }

  @Override
  public int run(String[] args) {
    if (args.length > 0) {
      try {
        numRows = Long.valueOf(args[0]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Number of rows parameter is invalid", e);
      }
      if (args.length > 1) {
        try {
          numMaps = Integer.valueOf(args[1]);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Number of mappers parameter is invalid", e);
        }
        if (args.length > 2) {
          outputDir = new Path(args[2]);
        }
      }
    }
    if (outputDir == null) {
      throw new IllegalArgumentException("Output directory not specified");
    }
    int res = runLoader(numRows, numMaps, outputDir);
    if (res != 0) {
      LOG.error("Loader failed");
      return -1;
    }
    res = runVerify(outputDir);
    if (res != 0) {
      LOG.error("Loader failed");
      return -1;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestLoadSmallValues(), args);
    System.exit(ret);
  }

  static class GeneratorInputFormat extends InputFormat<BytesWritable,MapWritable> {
    static final String GENERATOR_NUM_ROWS_KEY = "GeneratorRecordReader.numRows";
    static final BytesWritable LATITUDE = new BytesWritable(INFO_LATITUDE);
    static final BytesWritable LONGITUDE = new BytesWritable(INFO_LONGITUDE);
    static final BytesWritable TEMPERATURE = new BytesWritable(INFO_TEMPERATURE);

    static class GeneratorInputSplit extends InputSplit implements Writable {

      @Override
      public long getLength() throws IOException, InterruptedException {
        return 1;
      }

      @Override
      public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
      }

      @Override
      public void readFields(DataInput arg0) throws IOException { }

      @Override
      public void write(DataOutput arg0) throws IOException { }

    }

    static class GeneratorRecordReader extends RecordReader<BytesWritable,MapWritable> {
      private final Random r = new Random();
      private final RandomDistribution.Zipf latRng = new RandomDistribution.Zipf(r, 0, 90, 1.2);
      private final RandomDistribution.Zipf lonRng = new RandomDistribution.Zipf(r, 0, 180, 1.2);
      private final RandomDistribution.Zipf fracRng = new RandomDistribution.Zipf(r, 0, 999, 1.1);
      private long count;
      private long numRecords;
      private long ts;
      private int latitude, longitude;
      private short temperature;

      @Override
      public void close() throws IOException { }

      @Override
      public BytesWritable getCurrentKey() throws IOException, InterruptedException {
        final byte[] key = new byte[(Bytes.SIZEOF_INT * 2) + Bytes.SIZEOF_LONG];
        int off = 0;
        off = Bytes.putInt(key, off, latitude);
        off = Bytes.putInt(key, off, longitude);
        off = Bytes.putLong(key, off, ts);
        return new BytesWritable(key);
      }

      @Override
      public MapWritable getCurrentValue() throws IOException, InterruptedException {
        final MapWritable key = new MapWritable();
        key.put(LATITUDE, new BytesWritable(Bytes.toBytes(latitude)));
        key.put(LONGITUDE, new BytesWritable(Bytes.toBytes(longitude)));
        key.put(TEMPERATURE, new BytesWritable(Bytes.toBytes(temperature)));
        return key;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return (float)(count / (double)numRecords);
      }

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context)
          throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        numRecords = conf.getLong(GENERATOR_NUM_ROWS_KEY, DEFAULT_NUM_ROWS) /
          conf.getInt(Job.NUM_MAPS, DEFAULT_NUM_MAPS);
        next();
        LOG.info("Task {}: Generating {} records", context.getTaskAttemptID(), numRecords);
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        next();
        return count++ < numRecords;
      }

      private void next() {
        ts = getCurrentTime();
        // Fixed width representation, -90 / +90, scaled by 3 decimal places
        latitude = ((latRng.nextInt() * 1000) + fracRng.nextInt()) *
          (r.nextBoolean() ? 1 : -1);
        // Fixed width representation, -180 / +180, scaled by 3 decimal places
        longitude = ((lonRng.nextInt() * 1000) + fracRng.nextInt()) *
          (r.nextBoolean() ? 1 : -1);
        // -40 <= +40 C, approximately nine in ten measures are a positive value
        temperature = (short) (r.nextInt(40) * ((r.nextInt(10) == 1) ? -1 : +1));
      }

    }

    @Override
    public RecordReader<BytesWritable,MapWritable> createRecordReader(
        InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      GeneratorRecordReader rr = new GeneratorRecordReader();
      rr.initialize(split, context);
      return rr;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
      int numMappers = job.getConfiguration().getInt(Job.NUM_MAPS, DEFAULT_NUM_MAPS);
      LOG.info("Generating splits for {} mappers", numMappers);
      ArrayList<InputSplit> splits = new ArrayList<>(numMappers);
      for (int i = 0; i < numMappers; i++) {
        splits.add(new GeneratorInputSplit());
      }
      return splits;
    }
  }

  public static class Loader extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(Loader.class);
    private static final String USAGE = "Loader <numRows> <numMappers> <outputDir>";

    int run(final long numRows, final int numMaps, final Path outputDir) throws Exception {

      createSchema(getConf(), getTableName(getConf()));

      Job job = Job.getInstance(getConf());
      job.setJobName(Loader.class.getName());
      job.getConfiguration().setInt(Job.NUM_MAPS, numMaps);
      job.setNumReduceTasks(0);
      job.setJarByClass(getClass());
      job.setMapperClass(LoaderMapper.class);
      job.setInputFormatClass(GeneratorInputFormat.class);
      job.setOutputValueClass(MapWritable.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      SequenceFileOutputFormat.setOutputPath(job, outputDir);
      SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
      job.setOutputKeyClass(BytesWritable.class);
      job.setOutputValueClass(MapWritable.class);
      job.setSpeculativeExecution(false);
      job.getConfiguration().setLong(GeneratorInputFormat.GENERATOR_NUM_ROWS_KEY, numRows);
      TableMapReduceUtil.addDependencyJars(job);
      TableMapReduceUtil.addDependencyJarsForClasses(job.getConfiguration(),
        RandomDistribution.Zipf.class);

      boolean success = job.waitForCompletion(true);
      if (!success) {
        LOG.error("Failure during job " + job.getJobID());
      }
      return success ? 0 : 1;
    }

    @Override
    public int run(String[] args) throws Exception {
      if (args.length < 3) {
        System.err.println(USAGE);
        return 1;
      }
      final long numRows = Long.valueOf(args[0]);
      final int numMaps = Integer.valueOf(args[1]);
      final Path outputDir = new Path(args[2]);
      try {
        return run(numRows, numMaps, outputDir);
      } catch (NumberFormatException e) {
        System.err.println("Parsing loader arguments failed: " + e.getMessage());
        System.err.println(USAGE);
        return 1;
      }
    }

    public static void main(String[] args) throws Exception {
      System.exit(ToolRunner.run(HBaseConfiguration.create(), new Loader(), args));
    }

    static class LoaderMapper extends
        Mapper<BytesWritable, MapWritable, BytesWritable, MapWritable> {

      protected Configuration conf;
      protected Connection conn;
      protected BufferedMutator mutator;

      @Override
      protected void setup(final Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        conn = ConnectionFactory.createConnection(conf);
        mutator = conn.getBufferedMutator(getTableName(conf));
      }

      @Override
      protected void cleanup(final Context context) throws IOException, InterruptedException {
        try {
          mutator.close();
        } catch (Exception e) {
          LOG.warn("Exception closing Table", e);
        }
        try {
          conn.close();
        } catch (Exception e) {
          LOG.warn("Exception closing Connection", e);
        }
      }

      @Override
      protected void map(final BytesWritable key, final MapWritable value, final Context output)
          throws IOException, InterruptedException {
        // Write to HBase

        // Extract timestamp to use for our cells from the generated row key. Improves redundancy
        // in block data.
        final StringBuilder sb = new StringBuilder();
        final byte[] row = key.copyBytes();
        sb.append("row "); sb.append(Bytes.toStringBinary(row));
        final long ts = Bytes.toLong(row, Bytes.SIZEOF_INT * 2, Bytes.SIZEOF_LONG);
        sb.append(" ts "); sb.append(ts);
        final Put put = new Put(row);
        value.forEach((k,v) -> {
          final byte[] kb = ((BytesWritable)k).copyBytes();
          final byte[] vb = ((BytesWritable)v).copyBytes();
          sb.append(" "); sb.append(Bytes.toStringBinary(kb));
          sb.append(" "); sb.append(Bytes.toStringBinary(vb));
          put.addColumn(INFO_FAMILY_NAME, kb, ts, vb);
        });
        LOG.trace(sb.toString());
        mutator.mutate(put);

        // Write generator data to our MR output as well, for later verification.

        output.write(key, value);
      }

    }

  }

  public static class OneFilePerMapperSFIF<K, V> extends SequenceFileInputFormat<K, V> {
    @Override
    protected boolean isSplitable(final JobContext context, final Path filename) {
      return false;
    }
  }

  public static class Verify extends Configured implements Tool {

    public static final Logger LOG = LoggerFactory.getLogger(Verify.class);
    public static final String USAGE = "Verify <inputDir>";

    int run(final Path inputDir)
        throws IOException, ClassNotFoundException, InterruptedException {
      Job job = Job.getInstance(getConf());
      job.setJobName(Verify.class.getName());
      job.setJarByClass(getClass());
      job.setMapperClass(VerifyMapper.class);
      job.setInputFormatClass(OneFilePerMapperSFIF.class);
      FileInputFormat.setInputPaths(job, inputDir);
      job.setOutputFormatClass(NullOutputFormat.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NullWritable.class);
      TableMapReduceUtil.addDependencyJars(job);
      boolean success = job.waitForCompletion(true);
      if (!success) {
        LOG.error("Failure during job " + job.getJobID());
      }
      final Counters counters = job.getCounters();
      for (Counts c: Counts.values()) {
        LOG.info(c + ": " + counters.findCounter(c).getValue());
      }
      if (counters.findCounter(Counts.UNREFERENCED).getValue() > 0) {
        LOG.error("Nonzero UNREFERENCED count from job " + job.getJobID());
        success = false;
      }
      if (counters.findCounter(Counts.CORRUPT).getValue() > 0) {
        LOG.error("Nonzero CORRUPT count from job " + job.getJobID());
        success = false;
      }
      return success ? 0 : 1;
    }

    @Override
    public int run(String[] args) throws Exception {
      if (args.length < 1) {
        System.err.println(USAGE);
        return 1;
      }
      Path loaderOutput = new Path(args[0]);
      return run(loaderOutput);
    }

    public static void main(String[] args) throws Exception {
      System.exit(ToolRunner.run(HBaseConfiguration.create(), new Verify(), args));
    }

    public static class VerifyMapper
        extends Mapper<BytesWritable, MapWritable, NullWritable, NullWritable> {

      private Connection conn;
      private Table table;

      @Override
      protected void setup(final Context context) throws IOException, InterruptedException {
        conn = ConnectionFactory.createConnection(context.getConfiguration());
        table = conn.getTable(getTableName(conn.getConfiguration()));
      }

      @Override
      protected void cleanup(final Context context) throws IOException, InterruptedException {
        try {
          table.close();
        } catch (Exception e) {
          LOG.warn("Exception closing Table", e);
        }
        try {
          conn.close();
        } catch (Exception e) {
          LOG.warn("Exception closing Connection", e);
        }
      }

      @Override
      protected void map(final BytesWritable key, final MapWritable value, final Context output)
          throws IOException, InterruptedException {
        final byte[] row = key.copyBytes();
        final Get get = new Get(row);
        // For each expected value, add the qualifier to the query
        value.forEach((k,v) -> {
          get.addColumn(INFO_FAMILY_NAME, ((BytesWritable)k).copyBytes());
        });
        Result result = table.get(get);
        // Verify results
        // When updating counters, consider row by row
        value.forEach((k,v) -> {
          final byte[] b = result.getValue(INFO_FAMILY_NAME, ((BytesWritable)k).copyBytes());
          if (b == null) {
            LOG.error("Row '" + Bytes.toStringBinary(row) + "': missing value for " +
              Bytes.toStringBinary(((BytesWritable)k).copyBytes()));
            output.getCounter(Counts.UNREFERENCED).increment(1);
            return;
          }
          if (!Bytes.equals(b, ((BytesWritable)v).copyBytes())) {
            LOG.error("Row '" + Bytes.toStringBinary(row) + "': corrupt value for " +
              Bytes.toStringBinary(((BytesWritable)k).copyBytes()));
            output.getCounter(Counts.CORRUPT).increment(1);
            return;
          }
        });
        output.getCounter(Counts.REFERENCED).increment(1);
      }
    }

  }

  static TableName getTableName(Configuration conf) {
    return TableName.valueOf(conf.get(TABLE_NAME_KEY, DEFAULT_TABLE_NAME));
  }

  static void createSchema(final Configuration conf, final TableName tableName)
      throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(conf);
         Admin admin = conn.getAdmin()) {
      if (!admin.tableExists(tableName)) {
        ColumnFamilyDescriptorBuilder infoFamilyBuilder =
          ColumnFamilyDescriptorBuilder.newBuilder(INFO_FAMILY_NAME)
            .setBlocksize(DEFAULT_BLOCK_SIZE)
            .setDataBlockEncoding(DEFAULT_BLOCK_ENCODING)
            ;
        Set<ColumnFamilyDescriptor> families = new HashSet<>();
        families.add(infoFamilyBuilder.build());
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
            .setColumnFamilies(families)
            .build();
        if (conf.getBoolean(HBaseTestingUtil.PRESPLIT_TEST_TABLE_KEY, DEFAULT_PRESPLIT_TABLE)) {
          int numberOfServers = admin.getRegionServers().size();
          if (numberOfServers == 0) {
            throw new IllegalStateException("No live regionservers");
          }
          int regionsPerServer = conf.getInt(HBaseTestingUtil.REGIONS_PER_SERVER_KEY,
            HBaseTestingUtil.DEFAULT_REGIONS_PER_SERVER);
          int totalNumberOfRegions = numberOfServers * regionsPerServer;
          LOG.info("Creating test table: " + tableDescriptor);
          LOG.info("Number of live regionservers: " + numberOfServers + ", " +
              "pre-splitting table into " + totalNumberOfRegions + " regions " +
              "(default regions per server: " + regionsPerServer + ")");
          byte[][] splits = new RegionSplitter.UniformSplit().split(totalNumberOfRegions);
          admin.createTable(tableDescriptor, splits);
        } else {
          LOG.info("Creating test table: " + tableDescriptor);
          admin.createTable(tableDescriptor);
        }
      }
    } catch (MasterNotRunningException e) {
      LOG.error("Master not running", e);
      throw new IOException(e);
    }
  }

  static final AtomicLong counter = new AtomicLong();

  static long getCurrentTime() {
    // Typical hybrid logical clock scheme.
    // Take the current time, shift by 16 bits and zero those bits, and replace those bits
    // with the low 16 bits of the atomic counter. Mask off the high bit too because timestamps
    // cannot be negative.
    return ((EnvironmentEdgeManager.currentTime() << 16) & 0x7fff_ffff_ffff_0000L) |
        (counter.getAndIncrement() & 0xffffL);
  }

}
