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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
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
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.IntegrationTestBase;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerCallable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableRecordReaderImpl;
import org.apache.hadoop.hbase.mapreduce.WALPlayer;
import org.apache.hadoop.hbase.regionserver.FlushLargeStoresPolicy;
import org.apache.hadoop.hbase.regionserver.FlushPolicyFactory;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
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
  private static byte[] BIG_FAMILY_NAME = Bytes.toBytes("big");
  private static byte[] TINY_FAMILY_NAME = Bytes.toBytes("tiny");

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

  private static final int MISSING_ROWS_TO_LOG = 10; // YARN complains when too many counters

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

    /**
     * Set this configuration if you want to test single-column family flush works. If set, we will
     * add a big column family and a small column family on either side of the usual ITBLL 'meta'
     * column family. When we write out the ITBLL, we will also add to the big column family a value
     * bigger than that for ITBLL and for small, something way smaller. The idea is that when
     * flush-by-column family rather than by region is enabled, we can see if ITBLL is broke in any
     * way. Here is how you would pass it:
     * <p>
     * $ ./bin/hbase org.apache.hadoop.hbase.test.IntegrationTestBigLinkedList
     * -Dgenerator.multiple.columnfamilies=true generator 1 10 g
     */
    public static final String MULTIPLE_UNEVEN_COLUMNFAMILIES_KEY =
        "generator.multiple.columnfamilies";

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
          // Use SecureRandom to avoid issue described in HBASE-13382.
          rand = new SecureRandom();
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
     * <p>
     * [ . . . ] represents one batch of random longs of length WIDTH
     * <pre>
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
     * </pre>
     */
    static class GeneratorMapper
      extends Mapper<BytesWritable, NullWritable, NullWritable, NullWritable> {

      byte[][] first = null;
      byte[][] prev = null;
      byte[][] current = null;
      byte[] id;
      long count = 0;
      int i;
      BufferedMutator mutator;
      Connection connection;
      long numNodes;
      long wrap;
      int width;
      boolean multipleUnevenColumnFamilies;
      byte[] tinyValue = new byte[] { 't' };
      byte[] bigValue = null;

      @Override
      protected void setup(Context context) throws IOException, InterruptedException {
        id = Bytes.toBytes("Job: "+context.getJobID() + " Task: " + context.getTaskAttemptID());
        this.connection = ConnectionFactory.createConnection(context.getConfiguration());
        instantiateHTable();
        this.width = context.getConfiguration().getInt(GENERATOR_WIDTH_KEY, WIDTH_DEFAULT);
        current = new byte[this.width][];
        int wrapMultiplier = context.getConfiguration().getInt(GENERATOR_WRAP_KEY, WRAP_DEFAULT);
        this.wrap = (long)wrapMultiplier * width;
        this.numNodes = context.getConfiguration().getLong(
            GENERATOR_NUM_ROWS_PER_MAP_KEY, (long)WIDTH_DEFAULT * WRAP_DEFAULT);
        if (this.numNodes < this.wrap) {
          this.wrap = this.numNodes;
        }
        this.multipleUnevenColumnFamilies = isMultiUnevenColumnFamilies(context.getConfiguration());
      }

      protected void instantiateHTable() throws IOException {
        mutator = connection.getBufferedMutator(
            new BufferedMutatorParams(getTableName(connection.getConfiguration()))
                .writeBufferSize(4 * 1024 * 1024));
      }

      @Override
      protected void cleanup(Context context) throws IOException ,InterruptedException {
        mutator.close();
        connection.close();
      }

      @Override
      protected void map(BytesWritable key, NullWritable value, Context output) throws IOException {
        current[i] = new byte[key.getLength()];
        System.arraycopy(key.getBytes(), 0, current[i], 0, key.getLength());
        if (++i == current.length) {
          LOG.info("Persisting current.length=" + current.length + ", count=" + count + ", id=" +
            Bytes.toStringBinary(id) + ", current=" + Bytes.toStringBinary(current[0]) +
            ", i=" + i);
          persist(output, count, prev, current, id);
          i = 0;

          if (first == null) {
            first = current;
          }
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
          put.addColumn(FAMILY_NAME, COLUMN_PREV, prev == null ? NO_KEY : prev[i]);

          if (count >= 0) {
            put.addColumn(FAMILY_NAME, COLUMN_COUNT, Bytes.toBytes(count + i));
          }
          if (id != null) {
            put.addColumn(FAMILY_NAME, COLUMN_CLIENT, id);
          }
          // See if we are to write multiple columns.
          if (this.multipleUnevenColumnFamilies) {
            // Use any column name.
            put.addColumn(TINY_FAMILY_NAME, TINY_FAMILY_NAME, this.tinyValue);
            // If we've not allocated bigValue, do it now. Reuse same value each time.
            if (this.bigValue == null) {
              this.bigValue = new byte[current[i].length * 10];
              ThreadLocalRandom.current().nextBytes(this.bigValue);
            }
            // Use any column name.
            put.addColumn(BIG_FAMILY_NAME, BIG_FAMILY_NAME, this.bigValue);
          }
          mutator.mutate(put);

          if (i % 1000 == 0) {
            // Tickle progress every so often else maprunner will think us hung
            output.progress();
          }
        }

        mutator.flush();
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
      TableName tableName = getTableName(conf);
      try (Connection conn = ConnectionFactory.createConnection(conf);
          Admin admin = conn.getAdmin()) {
        if (!admin.tableExists(tableName)) {
          HTableDescriptor htd = new HTableDescriptor(getTableName(getConf()));
          htd.addFamily(new HColumnDescriptor(FAMILY_NAME));
          // Always add these families. Just skip writing to them when we do not test per CF flush.
          htd.addFamily(new HColumnDescriptor(BIG_FAMILY_NAME));
          htd.addFamily(new HColumnDescriptor(TINY_FAMILY_NAME));
          // if -DuseMob=true force all data through mob path.
          if (conf.getBoolean("useMob", false)) {
            for (HColumnDescriptor hcd : htd.getColumnFamilies() ) {
              hcd.setMobEnabled(true);
              hcd.setMobThreshold(4);
            }
          }

          // If we want to pre-split compute how many splits.
          if (conf.getBoolean(HBaseTestingUtility.PRESPLIT_TEST_TABLE_KEY,
              HBaseTestingUtility.PRESPLIT_TEST_TABLE)) {
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


            byte[][] splits = new RegionSplitter.UniformSplit().split(totalNumberOfRegions);

            admin.createTable(htd, splits);
          } else {
            // Looks like we're just letting things play out.
            // Create a table with on region by default.
            // This will make the splitting work hard.
            admin.createTable(htd);
          }
        }
      } catch (MasterNotRunningException e) {
        LOG.error("Master not running", e);
        throw new IOException(e);
      }
    }

    public int runRandomInputGenerator(int numMappers, long numNodes, Path tmpOutput,
        Integer width, Integer wrapMuplitplier) throws Exception {
      LOG.info("Running RandomInputGenerator with numMappers=" + numMappers
          + ", numNodes=" + numNodes);
      Job job = Job.getInstance(getConf());

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
      Job job = Job.getInstance(getConf());

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

      job.getConfiguration().setBoolean("mapreduce.map.speculative", false);
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
   * Tool to search missing rows in WALs and hfiles.
   * Pass in file or dir of keys to search for. Key file must have been written by Verify step
   * (we depend on the format it writes out. We'll read them in and then search in hbase
   * WALs and oldWALs dirs (Some of this is TODO).
   */
  static class Search extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(Search.class);
    protected Job job;

    private static void printUsage(final String error) {
      if (error != null && error.length() > 0) System.out.println("ERROR: " + error);
      System.err.println("Usage: search <KEYS_DIR> [<MAPPERS_COUNT>]");
    }

    @Override
    public int run(String[] args) throws Exception {
      if (args.length < 1 || args.length > 2) {
        printUsage(null);
        return 1;
      }
      Path inputDir = new Path(args[0]);
      int numMappers = 1;
      if (args.length > 1) {
        numMappers = Integer.parseInt(args[1]);
      }
      return run(inputDir, numMappers);
    }

    /**
     * WALPlayer override that searches for keys loaded in the setup.
     */
    public static class WALSearcher extends WALPlayer {
      public WALSearcher(Configuration conf) {
        super(conf);
      }

      /**
       * The actual searcher mapper.
       */
      public static class WALMapperSearcher extends WALMapper {
        private SortedSet<byte []> keysToFind;
        private AtomicInteger rows = new AtomicInteger(0);

        @Override
        public void setup(Mapper<WALKey, WALEdit, ImmutableBytesWritable, Mutation>.Context context)
            throws IOException {
          super.setup(context);
          try {
            this.keysToFind = readKeysToSearch(context.getConfiguration());
            LOG.info("Loaded keys to find: count=" + this.keysToFind.size());
          } catch (InterruptedException e) {
            throw new InterruptedIOException(e.toString());
          }
        }

        @Override
        protected boolean filter(Context context, Cell cell) {
          // TODO: Can I do a better compare than this copying out key?
          byte [] row = new byte [cell.getRowLength()];
          System.arraycopy(cell.getRowArray(), cell.getRowOffset(), row, 0, cell.getRowLength());
          boolean b = this.keysToFind.contains(row);
          if (b) {
            String keyStr = Bytes.toStringBinary(row);
            try {
              LOG.info("Found cell=" + cell + " , walKey=" + context.getCurrentKey());
            } catch (IOException|InterruptedException e) {
              LOG.warn(e);
            }
            if (rows.addAndGet(1) < MISSING_ROWS_TO_LOG) {
              context.getCounter(FOUND_GROUP_KEY, keyStr).increment(1);
            }
            context.getCounter(FOUND_GROUP_KEY, "CELL_WITH_MISSING_ROW").increment(1);
          }
          return b;
        }
      }

      // Put in place the above WALMapperSearcher.
      @Override
      public Job createSubmittableJob(String[] args) throws IOException {
        Job job = super.createSubmittableJob(args);
        // Call my class instead.
        job.setJarByClass(WALMapperSearcher.class);
        job.setMapperClass(WALMapperSearcher.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        return job;
      }
    }

    static final String FOUND_GROUP_KEY = "Found";
    static final String SEARCHER_INPUTDIR_KEY = "searcher.keys.inputdir";

    public int run(Path inputDir, int numMappers) throws Exception {
      getConf().set(SEARCHER_INPUTDIR_KEY, inputDir.toString());
      SortedSet<byte []> keys = readKeysToSearch(getConf());
      if (keys.isEmpty()) throw new RuntimeException("No keys to find");
      LOG.info("Count of keys to find: " + keys.size());
      for(byte [] key: keys)  LOG.info("Key: " + Bytes.toStringBinary(key));
      Path hbaseDir = new Path(getConf().get(HConstants.HBASE_DIR));
      // Now read all WALs. In two dirs. Presumes certain layout.
      Path walsDir = new Path(hbaseDir, HConstants.HREGION_LOGDIR_NAME);
      Path oldWalsDir = new Path(hbaseDir, HConstants.HREGION_OLDLOGDIR_NAME);
      LOG.info("Running Search with keys inputDir=" + inputDir +", numMappers=" + numMappers +
        " against " + getConf().get(HConstants.HBASE_DIR));
      int ret = ToolRunner.run(getConf(), new WALSearcher(getConf()),
          new String [] {walsDir.toString(), ""});
      if (ret != 0) {
        return ret;
      }
      return ToolRunner.run(getConf(), new WALSearcher(getConf()),
          new String [] {oldWalsDir.toString(), ""});
    }

    static SortedSet<byte []> readKeysToSearch(final Configuration conf)
    throws IOException, InterruptedException {
      Path keysInputDir = new Path(conf.get(SEARCHER_INPUTDIR_KEY));
      FileSystem fs = FileSystem.get(conf);
      SortedSet<byte []> result = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
      if (!fs.exists(keysInputDir)) {
        throw new FileNotFoundException(keysInputDir.toString());
      }
      if (!fs.isDirectory(keysInputDir)) {
        throw new UnsupportedOperationException("TODO");
      } else {
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(keysInputDir, false);
        while(iterator.hasNext()) {
          LocatedFileStatus keyFileStatus = iterator.next();
          // Skip "_SUCCESS" file.
          if (keyFileStatus.getPath().getName().startsWith("_")) continue;
          result.addAll(readFileToSearch(conf, fs, keyFileStatus));
        }
      }
      return result;
    }

    private static SortedSet<byte[]> readFileToSearch(final Configuration conf,
        final FileSystem fs, final LocatedFileStatus keyFileStatus) throws IOException,
        InterruptedException {
      SortedSet<byte []> result = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
      // Return entries that are flagged Counts.UNDEFINED in the value. Return the row. This is
      // what is missing.
      TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
      try (SequenceFileAsBinaryInputFormat.SequenceFileAsBinaryRecordReader rr =
          new SequenceFileAsBinaryInputFormat.SequenceFileAsBinaryRecordReader()) {
        InputSplit is =
          new FileSplit(keyFileStatus.getPath(), 0, keyFileStatus.getLen(), new String [] {});
        rr.initialize(is, context);
        while (rr.nextKeyValue()) {
          rr.getCurrentKey();
          BytesWritable bw = rr.getCurrentValue();
          if (Verify.VerifyReducer.whichType(bw.getBytes()) == Verify.Counts.UNDEFINED) {
            byte[] key = new byte[rr.getCurrentKey().getLength()];
            System.arraycopy(rr.getCurrentKey().getBytes(), 0, key, 0, rr.getCurrentKey()
                .getLength());
            result.add(key);
          }
        }
      }
      return result;
    }
  }

  /**
   * A Map Reduce job that verifies that the linked lists generated by
   * {@link Generator} do not have any holes.
   */
  static class Verify extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(Verify.class);
    protected static final BytesWritable DEF = new BytesWritable(new byte[] { 0 });
    protected static final BytesWritable DEF_LOST_FAMILIES = new BytesWritable(new byte[] { 1 });

    protected Job job;

    public static class VerifyMapper extends TableMapper<BytesWritable, BytesWritable> {
      private BytesWritable row = new BytesWritable();
      private BytesWritable ref = new BytesWritable();

      private boolean multipleUnevenColumnFamilies;

      @Override
      protected void setup(
          Mapper<ImmutableBytesWritable, Result, BytesWritable, BytesWritable>.Context context)
          throws IOException, InterruptedException {
        this.multipleUnevenColumnFamilies = isMultiUnevenColumnFamilies(context.getConfiguration());
      }

      @Override
      protected void map(ImmutableBytesWritable key, Result value, Context context)
          throws IOException ,InterruptedException {
        byte[] rowKey = key.get();
        row.set(rowKey, 0, rowKey.length);
        if (multipleUnevenColumnFamilies
            && (!value.containsColumn(BIG_FAMILY_NAME, BIG_FAMILY_NAME) || !value.containsColumn(
              TINY_FAMILY_NAME, TINY_FAMILY_NAME))) {
          context.write(row, DEF_LOST_FAMILIES);
        } else {
          context.write(row, DEF);
        }
        byte[] prev = value.getValue(FAMILY_NAME, COLUMN_PREV);
        if (prev != null && prev.length > 0) {
          ref.set(prev, 0, prev.length);
          context.write(ref, row);
        } else {
          LOG.warn(String.format("Prev is not set for: %s", Bytes.toStringBinary(rowKey)));
        }
      }
    }

    /**
     * Don't change the order of these enums. Their ordinals are used as type flag when we emit
     * problems found from the reducer.
     */
    public static enum Counts {
      UNREFERENCED, UNDEFINED, REFERENCED, CORRUPT, EXTRAREFERENCES, EXTRA_UNDEF_REFERENCES,
      LOST_FAMILIES
    }

    /**
     * Per reducer, we output problem rows as byte arrasy so can be used as input for
     * subsequent investigative mapreduce jobs. Each emitted value is prefaced by a one byte flag
     * saying what sort of emission it is. Flag is the Count enum ordinal as a short.
     */
    public static class VerifyReducer extends
        Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
      private ArrayList<byte[]> refs = new ArrayList<byte[]>();
      private final BytesWritable UNREF = new BytesWritable(addPrefixFlag(
        Counts.UNREFERENCED.ordinal(), new byte[] {}));
      private final BytesWritable LOSTFAM = new BytesWritable(addPrefixFlag(
        Counts.LOST_FAMILIES.ordinal(), new byte[] {}));

      private AtomicInteger rows = new AtomicInteger(0);
      private Connection connection;

      @Override
      protected void setup(Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable>.Context context)
      throws IOException, InterruptedException {
        super.setup(context);
        this.connection = ConnectionFactory.createConnection(context.getConfiguration());
      }

      @Override
      protected void cleanup(
          Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable>.Context context)
          throws IOException, InterruptedException {
        if (this.connection != null) {
          this.connection.close();
        }
        super.cleanup(context);
      }

      /**
       * @param ordinal
       * @param r
       * @return Return new byte array that has <code>ordinal</code> as prefix on front taking up
       * Bytes.SIZEOF_SHORT bytes followed by <code>r</code>
       */
      public static byte[] addPrefixFlag(final int ordinal, final byte [] r) {
        byte[] prefix = Bytes.toBytes((short)ordinal);
        if (prefix.length != Bytes.SIZEOF_SHORT) {
          throw new RuntimeException("Unexpected size: " + prefix.length);
        }
        byte[] result = new byte[prefix.length + r.length];
        System.arraycopy(prefix, 0, result, 0, prefix.length);
        System.arraycopy(r, 0, result, prefix.length, r.length);
        return result;
      }

      /**
       * @param bs
       * @return Type from the Counts enum of this row. Reads prefix added by
       * {@link #addPrefixFlag(int, byte[])}
       */
      public static Counts whichType(final byte [] bs) {
        int ordinal = Bytes.toShort(bs, 0, Bytes.SIZEOF_SHORT);
        return Counts.values()[ordinal];
      }

      /**
       * @param bw
       * @return Row bytes minus the type flag.
       */
      public static byte[] getRowOnly(BytesWritable bw) {
        byte[] bytes = new byte [bw.getLength() - Bytes.SIZEOF_SHORT];
        System.arraycopy(bw.getBytes(), Bytes.SIZEOF_SHORT, bytes, 0, bytes.length);
        return bytes;
      }

      @Override
      public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context)
          throws IOException, InterruptedException {
        int defCount = 0;
        boolean lostFamilies = false;
        refs.clear();
        for (BytesWritable type : values) {
          if (type.getLength() == DEF.getLength()) {
            defCount++;
            if (type.getBytes()[0] == 1) {
              lostFamilies = true;
            }
          } else {
            byte[] bytes = new byte[type.getLength()];
            System.arraycopy(type.getBytes(), 0, bytes, 0, type.getLength());
            refs.add(bytes);
          }
        }

        // TODO check for more than one def, should not happen
        StringBuilder refsSb = null;
        String keyString = Bytes.toStringBinary(key.getBytes(), 0, key.getLength());
        if (defCount == 0 || refs.size() != 1) {
          refsSb = dumpExtraInfoOnRefs(key, context, refs);
          LOG.error("LinkedListError: key=" + keyString + ", reference(s)=" +
            (refsSb != null? refsSb.toString(): ""));
        }
        if (lostFamilies) {
          LOG.error("LinkedListError: key=" + keyString + ", lost big or tiny families");
          context.getCounter(Counts.LOST_FAMILIES).increment(1);
          context.write(key, LOSTFAM);
        }

        if (defCount == 0 && refs.size() > 0) {
          // This is bad, found a node that is referenced but not defined. It must have been
          // lost, emit some info about this node for debugging purposes.
          // Write out a line per reference. If more than one, flag it.;
          for (int i = 0; i < refs.size(); i++) {
            byte[] bs = refs.get(i);
            int ordinal;
            if (i <= 0) {
              ordinal = Counts.UNDEFINED.ordinal();
              context.write(key, new BytesWritable(addPrefixFlag(ordinal, bs)));
              context.getCounter(Counts.UNDEFINED).increment(1);
            } else {
              ordinal = Counts.EXTRA_UNDEF_REFERENCES.ordinal();
              context.write(key, new BytesWritable(addPrefixFlag(ordinal, bs)));
            }
          }
          if (rows.addAndGet(1) < MISSING_ROWS_TO_LOG) {
            // Print out missing row; doing get on reference gives info on when the referencer
            // was added which can help a little debugging. This info is only available in mapper
            // output -- the 'Linked List error Key...' log message above. What we emit here is
            // useless for debugging.
            context.getCounter("undef", keyString).increment(1);
          }
        } else if (defCount > 0 && refs.size() == 0) {
          // node is defined but not referenced
          context.write(key, UNREF);
          context.getCounter(Counts.UNREFERENCED).increment(1);
          if (rows.addAndGet(1) < MISSING_ROWS_TO_LOG) {
            context.getCounter("unref", keyString).increment(1);
          }
        } else {
          if (refs.size() > 1) {
            // Skip first reference.
            for (int i = 1; i < refs.size(); i++) {
              context.write(key,
                new BytesWritable(addPrefixFlag(Counts.EXTRAREFERENCES.ordinal(), refs.get(i))));
            }
            context.getCounter(Counts.EXTRAREFERENCES).increment(refs.size() - 1);
          }
          // node is defined and referenced
          context.getCounter(Counts.REFERENCED).increment(1);
        }
      }

      /**
       * Dump out extra info around references if there are any. Helps debugging.
       * @return StringBuilder filled with references if any.
       * @throws IOException
       */
      private StringBuilder dumpExtraInfoOnRefs(final BytesWritable key, final Context context,
          final List<byte []> refs)
      throws IOException {
        StringBuilder refsSb = null;
        if (refs.isEmpty()) return refsSb;
        refsSb = new StringBuilder();
        String comma = "";
        // If a row is a reference but has no define, print the content of the row that has
        // this row as a 'prev'; it will help debug.  The missing row was written just before
        // the row we are dumping out here.
        TableName tn = getTableName(context.getConfiguration());
        try (Table t = this.connection.getTable(tn)) {
          for (byte [] ref : refs) {
            Result r = t.get(new Get(ref));
            List<Cell> cells = r.listCells();
            String ts = (cells != null && !cells.isEmpty())?
                new java.util.Date(cells.get(0).getTimestamp()).toString(): "";
            byte [] b = r.getValue(FAMILY_NAME, COLUMN_CLIENT);
            String jobStr = (b != null && b.length > 0)? Bytes.toString(b): "";
            b = r.getValue(FAMILY_NAME, COLUMN_COUNT);
            long count = (b != null && b.length > 0)? Bytes.toLong(b): -1;
            b = r.getValue(FAMILY_NAME, COLUMN_PREV);
            String refRegionLocation = "";
            String keyRegionLocation = "";
            if (b != null && b.length > 0) {
              try (RegionLocator rl = this.connection.getRegionLocator(tn)) {
                HRegionLocation hrl = rl.getRegionLocation(b);
                if (hrl != null) refRegionLocation = hrl.toString();
                // Key here probably has trailing zeros on it.
                hrl = rl.getRegionLocation(key.getBytes());
                if (hrl != null) keyRegionLocation = hrl.toString();
              }
            }
            LOG.error("Extras on ref without a def, ref=" + Bytes.toStringBinary(ref) +
              ", refPrevEqualsKey=" +
                (Bytes.compareTo(key.getBytes(), 0, key.getLength(), b, 0, b.length) == 0) +
                ", key=" + Bytes.toStringBinary(key.getBytes(), 0, key.getLength()) +
                ", ref row date=" + ts + ", jobStr=" + jobStr +
                ", ref row count=" + count +
                ", ref row regionLocation=" + refRegionLocation +
                ", key row regionLocation=" + keyRegionLocation);
            refsSb.append(comma);
            comma = ",";
            refsSb.append(Bytes.toStringBinary(ref));
          }
        }
        return refsSb;
      }
    }

    @Override
    public int run(String[] args) throws Exception {
      if (args.length != 2) {
        System.out.println("Usage : " + Verify.class.getSimpleName()
            + " <output dir> <num reducers>");
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

      job = Job.getInstance(getConf());

      job.setJobName("Link Verifier");
      job.setNumReduceTasks(numReducers);
      job.setJarByClass(getClass());

      setJobScannerConf(job);

      Scan scan = new Scan();
      scan.addColumn(FAMILY_NAME, COLUMN_PREV);
      scan.setCaching(10000);
      scan.setCacheBlocks(false);
      if (isMultiUnevenColumnFamilies(getConf())) {
        scan.addColumn(BIG_FAMILY_NAME, BIG_FAMILY_NAME);
        scan.addColumn(TINY_FAMILY_NAME, TINY_FAMILY_NAME);
      }

      TableMapReduceUtil.initTableMapperJob(getTableName(getConf()).getName(), scan,
          VerifyMapper.class, BytesWritable.class, BytesWritable.class, job);
      TableMapReduceUtil.addDependencyJars(job.getConfiguration(), AbstractHBaseTool.class);

      job.getConfiguration().setBoolean("mapreduce.map.speculative", false);

      job.setReducerClass(VerifyReducer.class);
      job.setOutputFormatClass(SequenceFileAsBinaryOutputFormat.class);
      job.setOutputKeyClass(BytesWritable.class);
      job.setOutputValueClass(BytesWritable.class);
      TextOutputFormat.setOutputPath(job, outputDir);

      boolean success = job.waitForCompletion(true);

      if (success) {
        Counters counters = job.getCounters();
        if (null == counters) {
          LOG.warn("Counters were null, cannot verify Job completion");
          // We don't have access to the counters to know if we have "bad" counts
          return 0;
        }

        // If we find no unexpected values, the job didn't outright fail
        if (verifyUnexpectedValues(counters)) {
          // We didn't check referenced+unreferenced counts, leave that to visual inspection
          return 0;
        }
      }

      // We failed
      return 1;
    }

    public boolean verify(long expectedReferenced) throws Exception {
      if (job == null) {
        throw new IllegalStateException("You should call run() first");
      }

      Counters counters = job.getCounters();

      // Run through each check, even if we fail one early
      boolean success = verifyExpectedValues(expectedReferenced, counters);

      if (!verifyUnexpectedValues(counters)) {
        // We found counter objects which imply failure
        success = false;
      }

      if (!success) {
        handleFailure(counters);
      }
      return success;
    }

    /**
     * Verify the values in the Counters against the expected number of entries written.
     *
     * @param expectedReferenced
     *          Expected number of referenced entrires
     * @param counters
     *          The Job's Counters object
     * @return True if the values match what's expected, false otherwise
     */
    protected boolean verifyExpectedValues(long expectedReferenced, Counters counters) {
      final Counter referenced = counters.findCounter(Counts.REFERENCED);
      final Counter unreferenced = counters.findCounter(Counts.UNREFERENCED);
      boolean success = true;

      if (expectedReferenced != referenced.getValue()) {
        LOG.error("Expected referenced count does not match with actual referenced count. " +
            "expected referenced=" + expectedReferenced + " ,actual=" + referenced.getValue());
        success = false;
      }

      if (unreferenced.getValue() > 0) {
        final Counter multiref = counters.findCounter(Counts.EXTRAREFERENCES);
        boolean couldBeMultiRef = (multiref.getValue() == unreferenced.getValue());
        LOG.error("Unreferenced nodes were not expected. Unreferenced count=" + unreferenced.getValue()
            + (couldBeMultiRef ? "; could be due to duplicate random numbers" : ""));
        success = false;
      }

      return success;
    }

    /**
     * Verify that the Counters don't contain values which indicate an outright failure from the Reducers.
     *
     * @param counters
     *          The Job's counters
     * @return True if the "bad" counter objects are 0, false otherwise
     */
    protected boolean verifyUnexpectedValues(Counters counters) {
      final Counter undefined = counters.findCounter(Counts.UNDEFINED);
      final Counter lostfamilies = counters.findCounter(Counts.LOST_FAMILIES);
      boolean success = true;

      if (undefined.getValue() > 0) {
        LOG.error("Found an undefined node. Undefined count=" + undefined.getValue());
        success = false;
      }

      if (lostfamilies.getValue() > 0) {
        LOG.error("Found nodes which lost big or tiny families, count=" + lostfamilies.getValue());
        success = false;
      }

      return success;
    }

    protected void handleFailure(Counters counters) throws IOException {
      Configuration conf = job.getConfiguration();
      TableName tableName = getTableName(conf);
      try (Connection conn = ConnectionFactory.createConnection(conf)) {
        try (RegionLocator rl = conn.getRegionLocator(tableName)) {
          CounterGroup g = counters.getGroup("undef");
          Iterator<Counter> it = g.iterator();
          while (it.hasNext()) {
            String keyString = it.next().getName();
            byte[] key = Bytes.toBytes(keyString);
            HRegionLocation loc = rl.getRegionLocation(key, true);
            LOG.error("undefined row " + keyString + ", " + loc);
          }
          g = counters.getGroup("unref");
          it = g.iterator();
          while (it.hasNext()) {
            String keyString = it.next().getName();
            byte[] key = Bytes.toBytes(keyString);
            HRegionLocation loc = rl.getRegionLocation(key, true);
            LOG.error("unreferred row " + keyString + ", " + loc);
          }
        }
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

      Connection connection = ConnectionFactory.createConnection(getConf());
      Table table = connection.getTable(getTableName(getConf()));

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
      connection.close();

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

      try (Connection connection = ConnectionFactory.createConnection(getConf());
          Table table = connection.getTable(getTableName(getConf()))) {
        table.delete(delete);
      }

      System.out.println("Delete successful");
      return 0;
    }
  }

  /**
   * A stand alone program that follows a linked list created by {@link Generator} and prints
   * timing info.
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
      Random rand = new SecureRandom();
      boolean isSpecificStart = cmd.hasOption('s');
      byte[] startKey = isSpecificStart ? Bytes.toBytesBinary(cmd.getOptionValue('s')) : null;
      int logEvery = cmd.hasOption('l') ? Integer.parseInt(cmd.getOptionValue('l')) : 1;

      Connection connection = ConnectionFactory.createConnection(getConf());
      Table table = connection.getTable(getTableName(getConf()));
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
      connection.close();
      return 0;
    }

    private static CINode findStartNode(Table table, byte[] startKey) throws IOException {
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

    private CINode getNode(byte[] row, Table table, CINode node) throws IOException {
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
      try (FileSystem fs = HFileSystem.get(conf);
          Connection conn = ConnectionFactory.createConnection(conf);
          Admin admin = conn.getAdmin()) {
        if (admin.tableExists(tableName)) {
          admin.disableTable(tableName);
          admin.deleteTable(tableName);
        }

        if (fs.exists(p)) {
          fs.delete(p, true);
        }
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

  private static boolean isMultiUnevenColumnFamilies(Configuration conf) {
    return conf.getBoolean(Generator.MULTIPLE_UNEVEN_COLUMNFAMILIES_KEY,true);
  }

  @Test
  public void testContinuousIngest() throws IOException, Exception {
    //Loop <num iterations> <num mappers> <num nodes per mapper> <output dir> <num reducers>
    Configuration conf = getTestingUtil(getConf()).getConfiguration();
    if (isMultiUnevenColumnFamilies(getConf())) {
      // make sure per CF flush is on
      conf.set(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY, FlushLargeStoresPolicy.class.getName());
    }
    int ret =
        ToolRunner.run(conf, new Loop(), new String[] { "1", "1", "2000000",
            util.getDataTestDirOnTestFS("IntegrationTestBigLinkedList").toString(), "1" });
    org.junit.Assert.assertEquals(0, ret);
  }

  private void usage() {
    System.err.println("Usage: " + this.getClass().getSimpleName() + " COMMAND [COMMAND options]");
    printCommands();
  }

  private void printCommands() {
    System.err.println("Commands:");
    System.err.println(" generator  Map only job that generates data.");
    System.err.println(" verify     A map reduce job that looks for holes. Check return code and");
    System.err.println("            look at the counts after running. See REFERENCED and");
    System.err.println("            UNREFERENCED are ok. Any UNDEFINED counts are bad. Do not run");
    System.err.println("            with the Generator.");
    System.err.println(" walker     " +
      "Standalone program that starts following a linked list & emits timing info.");
    System.err.println(" print      Standalone program that prints nodes in the linked list.");
    System.err.println(" delete     Standalone program that deletes a·single node.");
    System.err.println(" loop       Program to Loop through Generator and Verify steps");
    System.err.println(" clean      Program to clean all left over detritus.");
    System.err.println(" search     Search for missing keys.");
    System.err.println("");
    System.err.println("General options:");
    System.err.println(" -D"+ TABLE_NAME_KEY+ "=<tableName>");
    System.err.println("    Run using the <tableName> as the tablename.  Defaults to "
        + DEFAULT_TABLE_NAME);
    System.err.println(" -D"+ HBaseTestingUtility.REGIONS_PER_SERVER_KEY+ "=<# regions>");
    System.err.println("    Create table with presplit regions per server.  Defaults to "
        + HBaseTestingUtility.DEFAULT_REGIONS_PER_SERVER);

    System.err.println(" -DuseMob=<true|false>");
    System.err.println("    Create table so that the mob read/write path is forced.  " +
        "Defaults to false");

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
      // Have to throw an exception here to stop the processing. Looks ugly but gets message across.
      throw new RuntimeException("Incorrect Number of args.");
    }
    toRun = args[0];
    otherArgs = Arrays.copyOfRange(args, 1, args.length);
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    Tool tool = null;
    if (toRun.equalsIgnoreCase("Generator")) {
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
    } else if (toRun.equalsIgnoreCase("Search")) {
      tool = new Search();
    } else {
      usage();
      throw new RuntimeException("Unknown arg");
    }

    return ToolRunner.run(getConf(), tool, otherArgs);
  }

  @Override
  public TableName getTablename() {
    Configuration c = getConf();
    return TableName.valueOf(c.get(TABLE_NAME_KEY, DEFAULT_TABLE_NAME));
  }

  @Override
  protected Set<String> getColumnFamilies() {
    if (isMultiUnevenColumnFamilies(getConf())) {
      return Sets.newHashSet(Bytes.toString(FAMILY_NAME), Bytes.toString(BIG_FAMILY_NAME),
        Bytes.toString(TINY_FAMILY_NAME));
    } else {
      return Sets.newHashSet(Bytes.toString(FAMILY_NAME));
    }
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
