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

package org.apache.hadoop.hbase.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.IntegrationTestBase;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerCallable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableRecordReaderImpl;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

/**
 * This is an integration test borrowed from goraci, written by Keith Turner,
 * which is in turn inspired by the Accumulo test called continous ingest (ci).
 * The original source code can be found here:
 * https://github.com/keith-turner/goraci
 * https://github.com/enis/goraci/
 *
 * Apache Accumulo [0] has a simple test suite that verifies that data is not
 * lost at scale. This test suite is called continuous ingest. This test runs
 * many ingest clients that continually create linked lists containing 25
 * million nodes. At some point the clients are stopped and a map reduce job is
 * run to ensure no linked list has a hole. A hole indicates data was lost.··
 *
 * The nodes in the linked list are random. This causes each linked list to
 * spread across the table. Therefore if one part of a table loses data, then it
 * will be detected by references in another part of the table.
 *
 * THE ANATOMY OF THE TEST
 *
 * Below is rough sketch of how data is written. For specific details look at
 * the Generator code.
 *
 * 1 Write out 1 million nodes· 2 Flush the client· 3 Write out 1 million that
 * reference previous million· 4 If this is the 25th set of 1 million nodes,
 * then update 1st set of million to point to last· 5 goto 1
 *
 * The key is that nodes only reference flushed nodes. Therefore a node should
 * never reference a missing node, even if the ingest client is killed at any
 * point in time.
 *
 * When running this test suite w/ Accumulo there is a script running in
 * parallel called the Aggitator that randomly and continuously kills server
 * processes.·· The outcome was that many data loss bugs were found in Accumulo
 * by doing this.· This test suite can also help find bugs that impact uptime
 * and stability when· run for days or weeks.··
 *
 * This test suite consists the following· - a few Java programs· - a little
 * helper script to run the java programs - a maven script to build it.··
 *
 * When generating data, its best to have each map task generate a multiple of
 * 25 million. The reason for this is that circular linked list are generated
 * every 25M. Not generating a multiple in 25M will result in some nodes in the
 * linked list not having references. The loss of an unreferenced node can not
 * be detected.
 *
 *
 * Below is a description of the Java programs
 *
 * Generator - A map only job that generates data. As stated previously,·
 * its best to generate data in multiples of 25M.
 *
 * Verify - A map reduce job that looks for holes. Look at the counts after running. REFERENCED and
 * UNREFERENCED are· ok, any UNDEFINED counts are bad. Do not run at the· same
 * time as the Generator.
 *
 * Walker - A standalone program that start following a linked list· and emits timing info.··
 *
 * Print - A standalone program that prints nodes in the linked list
 *
 * Delete - A standalone program that deletes a single node
 *
 * This class can be run as a unit test, as an integration test, or from the command line
 */
@Category(IntegrationTests.class)
public class IntegrationTestBigLinkedList extends IntegrationTestBase {
  protected static final byte[] NO_KEY = new byte[1];

  protected static String TABLE_NAME_KEY = "IntegrationTestBigLinkedList.table";

  protected static String DEFAULT_TABLE_NAME = "IntegrationTestBigLinkedList";

  protected static byte[] FAMILY_NAME = Bytes.toBytes("meta");

  //link to the id of the prev node in the linked list
  protected static final byte[] COLUMN_PREV = Bytes.toBytes("prev");

  //identifier of the mapred task that generated this row
  protected static final byte[] COLUMN_CLIENT = Bytes.toBytes("client");

  //the id of the row within the same client.
  protected static final byte[] COLUMN_COUNT = Bytes.toBytes("count");

  /** How many rows to write per map task. This has to be a multiple of 25M */
  private static final String GENERATOR_NUM_ROWS_PER_MAP_KEY
    = "IntegrationTestBigLinkedList.generator.num_rows";

  private static final String GENERATOR_NUM_MAPPERS_KEY
    = "IntegrationTestBigLinkedList.generator.map.tasks";

  private static final String GENERATOR_WIDTH_KEY
    = "IntegrationTestBigLinkedList.generator.width";

  private static final String GENERATOR_WRAP_KEY
    = "IntegrationTestBigLinkedList.generator.wrap";

  protected int NUM_SLAVES_BASE = 3; // number of slaves for the cluster

  private static final int MISSING_ROWS_TO_LOG = 50;

  private static final int WIDTH_DEFAULT = 1000000;
  private static final int WRAP_DEFAULT = 25;
  private static final int ROWKEY_LENGTH = 16;

  protected String toRun;
  protected String[] otherArgs;

  static class CINode {
    byte[] key;
    byte[] prev;
    String client;
    long count;
  }

  /**
   * A Map only job that generates random linked list and stores them.
   */
  static class Generator extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(Generator.class);

    static class GeneratorInputFormat extends InputFormat<BytesWritable,NullWritable> {
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
        public void readFields(DataInput arg0) throws IOException {
        }
        @Override
        public void write(DataOutput arg0) throws IOException {
        }
      }

      static class GeneratorRecordReader extends RecordReader<BytesWritable,NullWritable> {
        private long count;
        private long numNodes;
        private Random rand;

        @Override
        public void close() throws IOException {
        }

        @Override
        public BytesWritable getCurrentKey() throws IOException, InterruptedException {
          byte[] bytes = new byte[ROWKEY_LENGTH];
          rand.nextBytes(bytes);
          return new BytesWritable(bytes);
        }

        @Override
        public NullWritable getCurrentValue() throws IOException, InterruptedException {
          return NullWritable.get();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
          return (float)(count / (double)numNodes);
        }

        @Override
        public void initialize(InputSplit arg0, TaskAttemptContext context)
            throws IOException, InterruptedException {
          numNodes = context.getConfiguration().getLong(GENERATOR_NUM_ROWS_PER_MAP_KEY, 25000000);
          rand = new Random();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
          return count++ < numNodes;
        }

      }

      @Override
      public RecordReader<BytesWritable,NullWritable> createRecordReader(
          InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        GeneratorRecordReader rr = new GeneratorRecordReader();
        rr.initialize(split, context);
        return rr;
      }

      @Override
      public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
        int numMappers = job.getConfiguration().getInt(GENERATOR_NUM_MAPPERS_KEY, 1);

        ArrayList<InputSplit> splits = new ArrayList<InputSplit>(numMappers);

        for (int i = 0; i < numMappers; i++) {
          splits.add(new GeneratorInputSplit());
        }

        return splits;
      }
    }

    /** Ensure output files from prev-job go to map inputs for current job */
    static class OneFilePerMapperSFIF<K, V> extends SequenceFileInputFormat<K, V> {
      @Override
      protected boolean isSplitable(JobContext context, Path filename) {
        return false;
      }
    }

    /**
     * Some ASCII art time:
     * [ . . . ] represents one batch of random longs of length WIDTH
     *
     *                _________________________
     *               |                  ______ |
     *               |                 |      ||
     *             .-+-----------------+-----.||
     *             | |                 |     |||
     * first   = [ . . . . . . . . . . . ]   |||
     *             ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^     |||
     *             | | | | | | | | | | |     |||
     * prev    = [ . . . . . . . . . . . ]   |||
     *             ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^     |||
     *             | | | | | | | | | | |     |||
     * current = [ . . . . . . . . . . . ]   |||
     *                                       |||
     * ...                                   |||
     *                                       |||
     * last    = [ . . . . . . . . . . . ]   |||
     *             ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^_____|||
     *             |                 |________||
     *             |___________________________|
     */
    static class GeneratorMapper
      extends Mapper<BytesWritable, NullWritable, NullWritable, NullWritable> {

      byte[][] first = null;
      byte[][] prev = null;
      byte[][] current = null;
      byte[] id;
      long count = 0;
      int i;
      HTable table;
      long numNodes;
      long wrap;
      int width;

      @Override
      protected void setup(Context context) throws IOException, InterruptedException {
        id = Bytes.toBytes("Job: "+context.getJobID() + " Task: " + context.getTaskAttemptID());
        Configuration conf = context.getConfiguration();
        instantiateHTable(conf);
        this.width = context.getConfiguration().getInt(GENERATOR_WIDTH_KEY, WIDTH_DEFAULT);
        current = new byte[this.width][];
        int wrapMultiplier = context.getConfiguration().getInt(GENERATOR_WRAP_KEY, WRAP_DEFAULT);
        this.wrap = (long)wrapMultiplier * width;
        this.numNodes = context.getConfiguration().getLong(
            GENERATOR_NUM_ROWS_PER_MAP_KEY, (long)WIDTH_DEFAULT * WRAP_DEFAULT);
        if (this.numNodes < this.wrap) {
          this.wrap = this.numNodes;
        }
      }

      protected void instantiateHTable(Configuration conf) throws IOException {
        table = new HTable(conf, getTableName(conf));
        table.setAutoFlush(false, true);
        table.setWriteBufferSize(4 * 1024 * 1024);
      }

      @Override
      protected void cleanup(Context context) throws IOException ,InterruptedException {
        table.close();
      }

      @Override
      protected void map(BytesWritable key, NullWritable value, Context output) throws IOException {
        current[i] = new byte[key.getLength()];
        System.arraycopy(key.getBytes(), 0, current[i], 0, key.getLength());
        if (++i == current.length) {
          persist(output, count, prev, current, id);
          i = 0;

          if (first == null)
            first = current;
          prev = current;
          current = new byte[this.width][];

          count += current.length;
          output.setStatus("Count " + count);

          if (count % wrap == 0) {
            // this block of code turns the 1 million linked list of length 25 into one giant
            //circular linked list of 25 million
            circularLeftShift(first);

            persist(output, -1, prev, first, null);

            first = null;
            prev = null;
          }
        }
      }

      private static <T> void circularLeftShift(T[] first) {
        T ez = first[0];
        System.arraycopy(first, 1, first, 0, first.length - 1);
        first[first.length - 1] = ez;
      }

      protected void persist(Context output, long count, byte[][] prev, byte[][] current, byte[] id)
          throws IOException {
        for (int i = 0; i < current.length; i++) {
          Put put = new Put(current[i]);
          put.add(FAMILY_NAME, COLUMN_PREV, prev == null ? NO_KEY : prev[i]);

          if (count >= 0) {
            put.add(FAMILY_NAME, COLUMN_COUNT, Bytes.toBytes(count + i));
          }
          if (id != null) {
            put.add(FAMILY_NAME, COLUMN_CLIENT, id);
          }
          table.put(put);

          if (i % 1000 == 0) {
            // Tickle progress every so often else maprunner will think us hung
            output.progress();
          }
        }

        table.flushCommits();
      }
    }

    @Override
    public int run(String[] args) throws Exception {
      if (args.length < 3) {
        System.out.println("Usage : " + Generator.class.getSimpleName() +
            " <num mappers> <num nodes per map> <tmp output dir> [<width> <wrap multiplier>]");
        System.out.println("   where <num nodes per map> should be a multiple of " +
            " width*wrap multiplier, 25M by default");
        return 0;
      }

      int numMappers = Integer.parseInt(args[0]);
      long numNodes = Long.parseLong(args[1]);
      Path tmpOutput = new Path(args[2]);
      Integer width = (args.length < 4) ? null : Integer.parseInt(args[3]);
      Integer wrapMuplitplier = (args.length < 5) ? null : Integer.parseInt(args[4]);
      return run(numMappers, numNodes, tmpOutput, width, wrapMuplitplier);
    }

    protected void createSchema() throws IOException {
      Configuration conf = getConf();
      HBaseAdmin admin = new HBaseAdmin(conf);
      TableName tableName = getTableName(conf);
      try {
        if (!admin.tableExists(tableName)) {
          HTableDescriptor htd = new HTableDescriptor(getTableName(getConf()));
          htd.addFamily(new HColumnDescriptor(FAMILY_NAME));
          int numberOfServers = admin.getClusterStatus().getServers().size();
          if (numberOfServers == 0) {
            throw new IllegalStateException("No live regionservers");
          }
          int regionsPerServer = conf.getInt(HBaseTestingUtility.REGIONS_PER_SERVER_KEY,
                                HBaseTestingUtility.DEFAULT_REGIONS_PER_SERVER);
          int totalNumberOfRegions = numberOfServers * regionsPerServer;
          LOG.info("Number of live regionservers: " + numberOfServers + ", " +
              "pre-splitting table into " + totalNumberOfRegions + " regions " +
              "(default regions per server: " + regionsPerServer + ")");

          byte[][] splits = new RegionSplitter.UniformSplit().split(
              totalNumberOfRegions);

          admin.createTable(htd, splits);
        }
      } catch (MasterNotRunningException e) {
        LOG.error("Master not running", e);
        throw new IOException(e);
      } finally {
        admin.close();
      }
    }

    public int runRandomInputGenerator(int numMappers, long numNodes, Path tmpOutput,
        Integer width, Integer wrapMuplitplier) throws Exception {
      LOG.info("Running RandomInputGenerator with numMappers=" + numMappers
          + ", numNodes=" + numNodes);
      Job job = new Job(getConf());

      job.setJobName("Random Input Generator");
      job.setNumReduceTasks(0);
      job.setJarByClass(getClass());

      job.setInputFormatClass(GeneratorInputFormat.class);
      job.setOutputKeyClass(BytesWritable.class);
      job.setOutputValueClass(NullWritable.class);

      setJobConf(job, numMappers, numNodes, width, wrapMuplitplier);

      job.setMapperClass(Mapper.class); //identity mapper

      FileOutputFormat.setOutputPath(job, tmpOutput);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);

      boolean success = jobCompletion(job);

      return success ? 0 : 1;
    }

    public int runGenerator(int numMappers, long numNodes, Path tmpOutput,
        Integer width, Integer wrapMuplitplier) throws Exception {
      LOG.info("Running Generator with numMappers=" + numMappers +", numNodes=" + numNodes);
      createSchema();
      Job job = new Job(getConf());

      job.setJobName("Link Generator");
      job.setNumReduceTasks(0);
      job.setJarByClass(getClass());

      FileInputFormat.setInputPaths(job, tmpOutput);
      job.setInputFormatClass(OneFilePerMapperSFIF.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NullWritable.class);

      setJobConf(job, numMappers, numNodes, width, wrapMuplitplier);

      setMapperForGenerator(job);

      job.setOutputFormatClass(NullOutputFormat.class);

      job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
      TableMapReduceUtil.addDependencyJars(job);
      TableMapReduceUtil.addDependencyJars(job.getConfiguration(), AbstractHBaseTool.class);
      TableMapReduceUtil.initCredentials(job);

      boolean success = jobCompletion(job);

      return success ? 0 : 1;
    }

    protected boolean jobCompletion(Job job) throws IOException, InterruptedException,
        ClassNotFoundException {
      boolean success = job.waitForCompletion(true);
      return success;
    }

    protected void setMapperForGenerator(Job job) {
      job.setMapperClass(GeneratorMapper.class);
    }

    public int run(int numMappers, long numNodes, Path tmpOutput,
        Integer width, Integer wrapMuplitplier) throws Exception {
      int ret = runRandomInputGenerator(numMappers, numNodes, tmpOutput, width, wrapMuplitplier);
      if (ret > 0) {
        return ret;
      }
      return runGenerator(numMappers, numNodes, tmpOutput, width, wrapMuplitplier);
    }
  }

  /**
   * A Map Reduce job that verifies that the linked lists generated by
   * {@link Generator} do not have any holes.
   */
  static class Verify extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(Verify.class);
    protected static final BytesWritable DEF = new BytesWritable(NO_KEY);

    protected Job job;

    public static class VerifyMapper extends TableMapper<BytesWritable, BytesWritable> {
      private BytesWritable row = new BytesWritable();
      private BytesWritable ref = new BytesWritable();

      @Override
      protected void map(ImmutableBytesWritable key, Result value, Context context)
          throws IOException ,InterruptedException {
        byte[] rowKey = key.get();
        row.set(rowKey, 0, rowKey.length);
        context.write(row, DEF);
        byte[] prev = value.getValue(FAMILY_NAME, COLUMN_PREV);
        if (prev != null && prev.length > 0) {
          ref.set(prev, 0, prev.length);
          context.write(ref, row);
        } else {
          LOG.warn(String.format("Prev is not set for: %s", Bytes.toStringBinary(rowKey)));
        }
      }
    }

    public static enum Counts {
      UNREFERENCED, UNDEFINED, REFERENCED, CORRUPT, EXTRAREFERENCES
    }

    public static class VerifyReducer extends Reducer<BytesWritable,BytesWritable,Text,Text> {
      private ArrayList<byte[]> refs = new ArrayList<byte[]>();

      private AtomicInteger rows = new AtomicInteger(0);

      @Override
      public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context)
          throws IOException, InterruptedException {

        int defCount = 0;

        refs.clear();
        for (BytesWritable type : values) {
          if (type.getLength() == DEF.getLength()) {
            defCount++;
          } else {
            byte[] bytes = new byte[type.getLength()];
            System.arraycopy(type.getBytes(), 0, bytes, 0, type.getLength());
            refs.add(bytes);
          }
        }

        // TODO check for more than one def, should not happen

        StringBuilder refsSb = null;
        String keyString = null;
        if (defCount == 0 || refs.size() != 1) {
          refsSb = new StringBuilder();
          String comma = "";
          for (byte[] ref : refs) {
            refsSb.append(comma);
            comma = ",";
            refsSb.append(Bytes.toStringBinary(ref));
          }
          keyString = Bytes.toStringBinary(key.getBytes(), 0, key.getLength());

          LOG.error("Linked List error: Key = " + keyString + " References = " + refsSb.toString());
        }

        if (defCount == 0 && refs.size() > 0) {
          // this is bad, found a node that is referenced but not defined. It must have been
          // lost, emit some info about this node for debugging purposes.
          context.write(new Text(keyString), new Text(refsSb.toString()));
          context.getCounter(Counts.UNDEFINED).increment(1);
          if (rows.addAndGet(1) < MISSING_ROWS_TO_LOG) {
            context.getCounter("undef", keyString).increment(1);
          }
        } else if (defCount > 0 && refs.size() == 0) {
          // node is defined but not referenced
          context.write(new Text(keyString), new Text("none"));
          context.getCounter(Counts.UNREFERENCED).increment(1);
          if (rows.addAndGet(1) < MISSING_ROWS_TO_LOG) {
            context.getCounter("unref", keyString).increment(1);
          }
        } else {
          if (refs.size() > 1) {
            if (refsSb != null) {
              context.write(new Text(keyString), new Text(refsSb.toString()));
            }
            context.getCounter(Counts.EXTRAREFERENCES).increment(refs.size() - 1);
          }
          // node is defined and referenced
          context.getCounter(Counts.REFERENCED).increment(1);
        }

      }
    }

    @Override
    public int run(String[] args) throws Exception {

      if (args.length != 2) {
        System.out.println("Usage : " + Verify.class.getSimpleName() + " <output dir> <num reducers>");
        return 0;
      }

      String outputDir = args[0];
      int numReducers = Integer.parseInt(args[1]);

       return run(outputDir, numReducers);
    }

    public int run(String outputDir, int numReducers) throws Exception {
      return run(new Path(outputDir), numReducers);
    }

    public int run(Path outputDir, int numReducers) throws Exception {
      LOG.info("Running Verify with outputDir=" + outputDir +", numReducers=" + numReducers);

      job = new Job(getConf());

      job.setJobName("Link Verifier");
      job.setNumReduceTasks(numReducers);
      job.setJarByClass(getClass());

      setJobScannerConf(job);

      Scan scan = new Scan();
      scan.addColumn(FAMILY_NAME, COLUMN_PREV);
      scan.setCaching(10000);
      scan.setCacheBlocks(false);

      TableMapReduceUtil.initTableMapperJob(getTableName(getConf()).getName(), scan,
          VerifyMapper.class, BytesWritable.class, BytesWritable.class, job);
      TableMapReduceUtil.addDependencyJars(job.getConfiguration(), AbstractHBaseTool.class);

      job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);

      job.setReducerClass(VerifyReducer.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      TextOutputFormat.setOutputPath(job, outputDir);

      boolean success = job.waitForCompletion(true);

      return success ? 0 : 1;
    }

    @SuppressWarnings("deprecation")
    public boolean verify(long expectedReferenced) throws Exception {
      if (job == null) {
        throw new IllegalStateException("You should call run() first");
      }

      Counters counters = job.getCounters();

      Counter referenced = counters.findCounter(Counts.REFERENCED);
      Counter unreferenced = counters.findCounter(Counts.UNREFERENCED);
      Counter undefined = counters.findCounter(Counts.UNDEFINED);
      Counter multiref = counters.findCounter(Counts.EXTRAREFERENCES);

      boolean success = true;
      //assert
      if (expectedReferenced != referenced.getValue()) {
        LOG.error("Expected referenced count does not match with actual referenced count. " +
            "expected referenced=" + expectedReferenced + " ,actual=" + referenced.getValue());
        success = false;
      }

      if (unreferenced.getValue() > 0) {
        boolean couldBeMultiRef = (multiref.getValue() == unreferenced.getValue());
        LOG.error("Unreferenced nodes were not expected. Unreferenced count=" + unreferenced.getValue()
            + (couldBeMultiRef ? "; could be due to duplicate random numbers" : ""));
        success = false;
      }

      if (undefined.getValue() > 0) {
        LOG.error("Found an undefined node. Undefined count=" + undefined.getValue());
        success = false;
      }

      if (!success) {
        handleFailure(counters);
      }
      return success;
    }

    protected void handleFailure(Counters counters) throws IOException {
      Configuration conf = job.getConfiguration();
      HConnection conn = HConnectionManager.getConnection(conf);
      TableName tableName = getTableName(conf);
      CounterGroup g = counters.getGroup("undef");
      Iterator<Counter> it = g.iterator();
      while (it.hasNext()) {
        String keyString = it.next().getName();
        byte[] key = Bytes.toBytes(keyString);
        HRegionLocation loc = conn.relocateRegion(tableName, key);
        LOG.error("undefined row " + keyString + ", " + loc);
      }
      g = counters.getGroup("unref");
      it = g.iterator();
      while (it.hasNext()) {
        String keyString = it.next().getName();
        byte[] key = Bytes.toBytes(keyString);
        HRegionLocation loc = conn.relocateRegion(tableName, key);
        LOG.error("unreferred row " + keyString + ", " + loc);
      }
    }
  }

  /**
   * Executes Generate and Verify in a loop. Data is not cleaned between runs, so each iteration
   * adds more data.
   */
  static class Loop extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(Loop.class);

    IntegrationTestBigLinkedList it;

    protected void runGenerator(int numMappers, long numNodes,
        String outputDir, Integer width, Integer wrapMuplitplier) throws Exception {
      Path outputPath = new Path(outputDir);
      UUID uuid = UUID.randomUUID(); //create a random UUID.
      Path generatorOutput = new Path(outputPath, uuid.toString());

      Generator generator = new Generator();
      generator.setConf(getConf());
      int retCode = generator.run(numMappers, numNodes, generatorOutput, width, wrapMuplitplier);
      if (retCode > 0) {
        throw new RuntimeException("Generator failed with return code: " + retCode);
      }
    }

    protected void runVerify(String outputDir,
        int numReducers, long expectedNumNodes) throws Exception {
      Path outputPath = new Path(outputDir);
      UUID uuid = UUID.randomUUID(); //create a random UUID.
      Path iterationOutput = new Path(outputPath, uuid.toString());

      Verify verify = new Verify();
      verify.setConf(getConf());
      int retCode = verify.run(iterationOutput, numReducers);
      if (retCode > 0) {
        throw new RuntimeException("Verify.run failed with return code: " + retCode);
      }

      if (!verify.verify(expectedNumNodes)) {
        throw new RuntimeException("Verify.verify failed");
      }

      LOG.info("Verify finished with succees. Total nodes=" + expectedNumNodes);
    }

    @Override
    public int run(String[] args) throws Exception {
      if (args.length < 5) {
        System.err.println("Usage: Loop <num iterations> <num mappers> <num nodes per mapper> <output dir> <num reducers> [<width> <wrap multiplier>]");
        return 1;
      }
      LOG.info("Running Loop with args:" + Arrays.deepToString(args));

      int numIterations = Integer.parseInt(args[0]);
      int numMappers = Integer.parseInt(args[1]);
      long numNodes = Long.parseLong(args[2]);
      String outputDir = args[3];
      int numReducers = Integer.parseInt(args[4]);
      Integer width = (args.length < 6) ? null : Integer.parseInt(args[5]);
      Integer wrapMuplitplier = (args.length < 7) ? null : Integer.parseInt(args[6]);

      long expectedNumNodes = 0;

      if (numIterations < 0) {
        numIterations = Integer.MAX_VALUE; //run indefinitely (kind of)
      }

      for (int i = 0; i < numIterations; i++) {
        LOG.info("Starting iteration = " + i);
        runGenerator(numMappers, numNodes, outputDir, width, wrapMuplitplier);
        expectedNumNodes += numMappers * numNodes;

        runVerify(outputDir, numReducers, expectedNumNodes);
      }

      return 0;
    }
  }

  /**
   * A stand alone program that prints out portions of a list created by {@link Generator}
   */
  private static class Print extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
      Options options = new Options();
      options.addOption("s", "start", true, "start key");
      options.addOption("e", "end", true, "end key");
      options.addOption("l", "limit", true, "number to print");

      GnuParser parser = new GnuParser();
      CommandLine cmd = null;
      try {
        cmd = parser.parse(options, args);
        if (cmd.getArgs().length != 0) {
          throw new ParseException("Command takes no arguments");
        }
      } catch (ParseException e) {
        System.err.println("Failed to parse command line " + e.getMessage());
        System.err.println();
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(getClass().getSimpleName(), options);
        System.exit(-1);
      }

      HTable table = new HTable(getConf(), getTableName(getConf()));

      Scan scan = new Scan();
      scan.setBatch(10000);

      if (cmd.hasOption("s"))
        scan.setStartRow(Bytes.toBytesBinary(cmd.getOptionValue("s")));

      if (cmd.hasOption("e"))
        scan.setStopRow(Bytes.toBytesBinary(cmd.getOptionValue("e")));

      int limit = 0;
      if (cmd.hasOption("l"))
        limit = Integer.parseInt(cmd.getOptionValue("l"));
      else
        limit = 100;

      ResultScanner scanner = table.getScanner(scan);

      CINode node = new CINode();
      Result result = scanner.next();
      int count = 0;
      while (result != null && count++ < limit) {
        node = getCINode(result, node);
        System.out.printf("%s:%s:%012d:%s\n", Bytes.toStringBinary(node.key),
            Bytes.toStringBinary(node.prev), node.count, node.client);
        result = scanner.next();
      }
      scanner.close();
      table.close();

      return 0;
    }
  }

  /**
   * A stand alone program that deletes a single node.
   */
  private static class Delete extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
      if (args.length != 1) {
        System.out.println("Usage : " + Delete.class.getSimpleName() + " <node to delete>");
        return 0;
      }
      byte[] val = Bytes.toBytesBinary(args[0]);

      org.apache.hadoop.hbase.client.Delete delete
        = new org.apache.hadoop.hbase.client.Delete(val);

      HTable table = new HTable(getConf(), getTableName(getConf()));

      table.delete(delete);
      table.flushCommits();
      table.close();

      System.out.println("Delete successful");
      return 0;
    }
  }

  /**
   * A stand alone program that follows a linked list created by {@link Generator} and prints timing info.
   */
  private static class Walker extends Configured implements Tool {
    @Override
    public int run(String[] args) throws IOException {
      Options options = new Options();
      options.addOption("n", "num", true, "number of queries");
      options.addOption("s", "start", true, "key to start at, binary string");
      options.addOption("l", "logevery", true, "log every N queries");

      GnuParser parser = new GnuParser();
      CommandLine cmd = null;
      try {
        cmd = parser.parse(options, args);
        if (cmd.getArgs().length != 0) {
          throw new ParseException("Command takes no arguments");
        }
      } catch (ParseException e) {
        System.err.println("Failed to parse command line " + e.getMessage());
        System.err.println();
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(getClass().getSimpleName(), options);
        System.exit(-1);
      }

      long maxQueries = Long.MAX_VALUE;
      if (cmd.hasOption('n')) {
        maxQueries = Long.parseLong(cmd.getOptionValue("n"));
      }
      Random rand = new Random();
      boolean isSpecificStart = cmd.hasOption('s');
      byte[] startKey = isSpecificStart ? Bytes.toBytesBinary(cmd.getOptionValue('s')) : null;
      int logEvery = cmd.hasOption('l') ? Integer.parseInt(cmd.getOptionValue('l')) : 1;

      HTable table = new HTable(getConf(), getTableName(getConf()));
      long numQueries = 0;
      // If isSpecificStart is set, only walk one list from that particular node.
      // Note that in case of circular (or P-shaped) list it will walk forever, as is
      // the case in normal run without startKey.
      while (numQueries < maxQueries && (numQueries == 0 || !isSpecificStart)) {
        if (!isSpecificStart) {
          startKey = new byte[ROWKEY_LENGTH];
          rand.nextBytes(startKey);
        }
        CINode node = findStartNode(table, startKey);
        if (node == null && isSpecificStart) {
          System.err.printf("Start node not found: %s \n", Bytes.toStringBinary(startKey));
        }
        numQueries++;
        while (node != null && node.prev.length != NO_KEY.length && numQueries < maxQueries) {
          byte[] prev = node.prev;
          long t1 = System.currentTimeMillis();
          node = getNode(prev, table, node);
          long t2 = System.currentTimeMillis();
          if (numQueries % logEvery == 0) {
            System.out.printf("CQ %d: %d %s \n", numQueries, t2 - t1, Bytes.toStringBinary(prev));
          }
          numQueries++;
          if (node == null) {
            System.err.printf("UNDEFINED NODE %s \n", Bytes.toStringBinary(prev));
          } else if (node.prev.length == NO_KEY.length) {
            System.err.printf("TERMINATING NODE %s \n", Bytes.toStringBinary(node.key));
          }
        }
      }

      table.close();
      return 0;
    }

    private static CINode findStartNode(HTable table, byte[] startKey) throws IOException {
      Scan scan = new Scan();
      scan.setStartRow(startKey);
      scan.setBatch(1);
      scan.addColumn(FAMILY_NAME, COLUMN_PREV);

      long t1 = System.currentTimeMillis();
      ResultScanner scanner = table.getScanner(scan);
      Result result = scanner.next();
      long t2 = System.currentTimeMillis();
      scanner.close();

      if ( result != null) {
        CINode node = getCINode(result, new CINode());
        System.out.printf("FSR %d %s\n", t2 - t1, Bytes.toStringBinary(node.key));
        return node;
      }

      System.out.println("FSR " + (t2 - t1));

      return null;
    }

    private CINode getNode(byte[] row, HTable table, CINode node) throws IOException {
      Get get = new Get(row);
      get.addColumn(FAMILY_NAME, COLUMN_PREV);
      Result result = table.get(get);
      return getCINode(result, node);
    }
  }

  private static class Clean extends Configured implements Tool {

    @Override public int run(String[] args) throws Exception {
      if (args.length < 1) {
        System.err.println("Usage: Clean <output dir>");
        return -1;
      }

      Path p = new Path(args[0]);
      Configuration conf = getConf();
      TableName tableName = getTableName(conf);

      FileSystem fs = HFileSystem.get(conf);
      HBaseAdmin admin = new HBaseAdmin(conf);
      try {
        if (admin.tableExists(tableName)) {
          admin.disableTable(tableName);
          admin.deleteTable(tableName);
        }
      } finally {
        admin.close();
      }

      if (fs.exists(p)) {
        fs.delete(p, true);
      }

      return 0;
    }
  }

  static TableName getTableName(Configuration conf) {
    return TableName.valueOf(conf.get(TABLE_NAME_KEY, DEFAULT_TABLE_NAME));
  }

  private static CINode getCINode(Result result, CINode node) {
    node.key = Bytes.copy(result.getRow());
    if (result.containsColumn(FAMILY_NAME, COLUMN_PREV)) {
      node.prev = Bytes.copy(result.getValue(FAMILY_NAME, COLUMN_PREV));
    } else {
      node.prev = NO_KEY;
    }
    if (result.containsColumn(FAMILY_NAME, COLUMN_COUNT)) {
      node.count = Bytes.toLong(result.getValue(FAMILY_NAME, COLUMN_COUNT));
    } else {
      node.count = -1;
    }
    if (result.containsColumn(FAMILY_NAME, COLUMN_CLIENT)) {
      node.client = Bytes.toString(result.getValue(FAMILY_NAME, COLUMN_CLIENT));
    } else {
      node.client = "";
    }
    return node;
  }

  protected IntegrationTestingUtility util;

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(getConf());
    boolean isDistributed = util.isDistributedCluster();
    util.initializeCluster(isDistributed ? 1 : this.NUM_SLAVES_BASE);
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

  @Test
  public void testContinuousIngest() throws IOException, Exception {
    //Loop <num iterations> <num mappers> <num nodes per mapper> <output dir> <num reducers>
    int ret = ToolRunner.run(getTestingUtil(getConf()).getConfiguration(), new Loop(),
        new String[] {"1", "1", "2000000",
                     util.getDataTestDirOnTestFS("IntegrationTestBigLinkedList").toString(), "1"});
    org.junit.Assert.assertEquals(0, ret);
  }

  private void usage() {
    System.err.println("Usage: " + this.getClass().getSimpleName() + " COMMAND [COMMAND options]");
    printCommands();
  }

  private void printCommands() {
    System.err.println("Commands:");
    System.err.println(" Generator  Map only job that generates data.");
    System.err.println(" Verify     A map reduce job that looks for holes. Look at the counts ");
    System.err.println("            after running. See REFERENCED and UNREFERENCED are ok. Any ");
    System.err.println("            UNDEFINED counts are bad. Do not run with the Generator.");
    System.err.println(" Walker     " +
      "Standalong program that starts following a linked list & emits timing info.");
    System.err.println(" Print      Standalone program that prints nodes in the linked list.");
    System.err.println(" Delete     Standalone program that deletes a·single node.");
    System.err.println(" Loop       Program to Loop through Generator and Verify steps");
    System.err.println(" Clean      Program to clean all left over detritus.");
    System.err.flush();
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    super.processOptions(cmd);
    String[] args = cmd.getArgs();
    //get the class, run with the conf
    if (args.length < 1) {
      printUsage(this.getClass().getSimpleName() +
        " <general options> COMMAND [<COMMAND options>]", "General options:", "");
      printCommands();
      throw new RuntimeException("Incorrect Number of args.");
    }
    toRun = args[0];
    otherArgs = Arrays.copyOfRange(args, 1, args.length);
  }

  @Override
  public int runTestFromCommandLine() throws Exception {

    Tool tool = null;
    if (toRun.equals("Generator")) {
      tool = new Generator();
    } else if (toRun.equalsIgnoreCase("Verify")) {
      tool = new Verify();
    } else if (toRun.equalsIgnoreCase("Loop")) {
      Loop loop = new Loop();
      loop.it = this;
      tool = loop;
    } else if (toRun.equalsIgnoreCase("Walker")) {
      tool = new Walker();
    } else if (toRun.equalsIgnoreCase("Print")) {
      tool = new Print();
    } else if (toRun.equalsIgnoreCase("Delete")) {
      tool = new Delete();
    } else if (toRun.equalsIgnoreCase("Clean")) {
      tool = new Clean();
    } else {
      usage();
      throw new RuntimeException("Unknown arg");
    }

    return ToolRunner.run(getConf(), tool, otherArgs);
  }

  @Override
  public String getTablename() {
    Configuration c = getConf();
    return c.get(TABLE_NAME_KEY, DEFAULT_TABLE_NAME);
  }

  @Override
  protected Set<String> getColumnFamilies() {
    return Sets.newHashSet(Bytes.toString(FAMILY_NAME));
  }

  private static void setJobConf(Job job, int numMappers, long numNodes,
      Integer width, Integer wrapMultiplier) {
    job.getConfiguration().setInt(GENERATOR_NUM_MAPPERS_KEY, numMappers);
    job.getConfiguration().setLong(GENERATOR_NUM_ROWS_PER_MAP_KEY, numNodes);
    if (width != null) {
      job.getConfiguration().setInt(GENERATOR_WIDTH_KEY, width);
    }
    if (wrapMultiplier != null) {
      job.getConfiguration().setInt(GENERATOR_WRAP_KEY, wrapMultiplier);
    }
  }

  public static void setJobScannerConf(Job job) {
    // Make sure scanners log something useful to make debugging possible.
    job.getConfiguration().setBoolean(ScannerCallable.LOG_SCANNER_ACTIVITY, true);
    job.getConfiguration().setInt(TableRecordReaderImpl.LOG_PER_ROW_COUNT, 100000);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestBigLinkedList(), args);
    System.exit(ret);
  }
}
