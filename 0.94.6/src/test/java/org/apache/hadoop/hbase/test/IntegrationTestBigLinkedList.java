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
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.IntegrationTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
 * Walker - A standalong program that start following a linked list· and emits timing info.··
 *
 * Print - A standalone program that prints nodes in the linked list
 *
 * Delete - A standalone program that deletes a single node
 *
 * This class can be run as a unit test, as an integration test, or from the command line
 */
@Category(IntegrationTests.class)
public class IntegrationTestBigLinkedList extends Configured implements Tool {

  private static final String TABLE_NAME_KEY = "IntegrationTestBigLinkedList.table";

  private static final String DEFAULT_TABLE_NAME = "ci";

  private static byte[] FAMILY_NAME = Bytes.toBytes("meta");

  //link to the id of the prev node in the linked list
  private static final byte[] COLUMN_PREV = Bytes.toBytes("prev");

  //identifier of the mapred task that generated this row
  private static final byte[] COLUMN_CLIENT = Bytes.toBytes("client");

  //the id of the row within the same client.
  private static final byte[] COLUMN_COUNT = Bytes.toBytes("count");

  /** How many rows to write per map task. This has to be a multiple of 25M */
  private static final String GENERATOR_NUM_ROWS_PER_MAP_KEY
    = "IntegrationTestBigLinkedList.generator.num_rows";

  private static final String GENERATOR_NUM_MAPPERS_KEY
    = "IntegrationTestBigLinkedList.generator.map.tasks";

  static class CINode {
    long key;
    long prev;
    String client;
    long count;
  }

  /**
   * A Map only job that generates random linked list and stores them.
   */
  static class Generator extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(Generator.class);

    private static final int WIDTH = 1000000;
    private static final int WRAP = WIDTH * 25;

    public static enum Counts {
      UNREFERENCED, UNDEFINED, REFERENCED, CORRUPT
    }

    static class GeneratorInputFormat extends InputFormat<LongWritable,NullWritable> {
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

      static class GeneratorRecordReader extends RecordReader<LongWritable,NullWritable> {
        private long count;
        private long numNodes;
        private Random rand;

        @Override
        public void close() throws IOException {
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
          return new LongWritable(Math.abs(rand.nextLong()));
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
      public RecordReader<LongWritable,NullWritable> createRecordReader(
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
     *             __+_________________+_____ ||
     *             v v                 v     |||
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
     *             | | | | | | | | | | |-----|||
     *             |                 |--------||
     *             |___________________________|
     */
    static class GeneratorMapper
      extends Mapper<LongWritable, NullWritable, NullWritable, NullWritable> {
      Random rand = new Random();

      long[] first = null;
      long[] prev = null;
      long[] current = new long[WIDTH];
      byte[] id;
      long count = 0;
      int i;
      HTable table;
      long numNodes;
      long wrap = WRAP;

      protected void setup(Context context) throws IOException, InterruptedException {
        id = Bytes.toBytes(UUID.randomUUID().toString());
        Configuration conf = context.getConfiguration();
        table = new HTable(conf, getTableName(conf));
        table.setAutoFlush(false);
        table.setWriteBufferSize(4 * 1024 * 1024);
        numNodes = context.getConfiguration().getLong(GENERATOR_NUM_MAPPERS_KEY, 25000000);
        if (numNodes < 25000000) {
          wrap = numNodes;
        }
      };

      protected void cleanup(Context context) throws IOException ,InterruptedException {
        table.close();
      };

      @Override
      protected void map(LongWritable key, NullWritable value, Context output) throws IOException {
        current[i++] = Math.abs(key.get());

        if (i == current.length) {
          persist(output, count, prev, current, id);
          i = 0;

          if (first == null)
            first = current;
          prev = current;
          current = new long[WIDTH];

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

      private static void circularLeftShift(long[] first) {
        long ez = first[0];
        for (int i = 0; i < first.length - 1; i++)
          first[i] = first[i + 1];
        first[first.length - 1] = ez;
      }

      private void persist(Context output, long count, long[] prev, long[] current, byte[] id)
          throws IOException {
        for (int i = 0; i < current.length; i++) {
          Put put = new Put(Bytes.toBytes(current[i]));
          put.add(FAMILY_NAME, COLUMN_PREV, Bytes.toBytes(prev == null ? -1 : prev[i]));

          if (count > 0) {
            put.add(FAMILY_NAME, COLUMN_COUNT, Bytes.toBytes(count + 1));
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
            " <num mappers> <num nodes per map> <tmp output dir>");
        System.out.println("   where <num nodes per map> should be a multiple of 25M");
        return 0;
      }

      int numMappers = Integer.parseInt(args[0]);
      long numNodes = Long.parseLong(args[1]);
      Path tmpOutput = new Path(args[2]);
      return run(numMappers, numNodes, tmpOutput);
    }

    protected void createSchema() throws IOException {
      HBaseAdmin admin = new HBaseAdmin(getConf());
      byte[] tableName = getTableName(getConf());
      if (!admin.tableExists(tableName)) {
        HTableDescriptor htd = new HTableDescriptor(getTableName(getConf()));
        htd.addFamily(new HColumnDescriptor(FAMILY_NAME));
        admin.createTable(htd);
      }
      admin.close();
    }

    public int runRandomInputGenerator(int numMappers, long numNodes, Path tmpOutput)
        throws Exception {
      LOG.info("Running RandomInputGenerator with numMappers=" + numMappers
          + ", numNodes=" + numNodes);
      Job job = new Job(getConf());

      job.setJobName("Random Input Generator");
      job.setNumReduceTasks(0);
      job.setJarByClass(getClass());

      job.setInputFormatClass(GeneratorInputFormat.class);
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(NullWritable.class);

      job.getConfiguration().setInt(GENERATOR_NUM_MAPPERS_KEY, numMappers);
      job.getConfiguration().setLong(GENERATOR_NUM_ROWS_PER_MAP_KEY, numNodes);

      job.setMapperClass(Mapper.class); //identity mapper

      FileOutputFormat.setOutputPath(job, tmpOutput);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);

      boolean success = job.waitForCompletion(true);

      return success ? 0 : 1;
    }

    public int runGenerator(int numMappers, long numNodes, Path tmpOutput) throws Exception {
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

      job.getConfiguration().setInt(GENERATOR_NUM_MAPPERS_KEY, numMappers);
      job.getConfiguration().setLong(GENERATOR_NUM_ROWS_PER_MAP_KEY, numNodes);

      job.setMapperClass(GeneratorMapper.class);

      job.setOutputFormatClass(NullOutputFormat.class);

      job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);

      boolean success = job.waitForCompletion(true);

      return success ? 0 : 1;
    }

    public int run(int numMappers, long numNodes, Path tmpOutput) throws Exception {
      int ret = runRandomInputGenerator(numMappers, numNodes, tmpOutput);
      if (ret > 0) {
        return ret;
      }

      return runGenerator(numMappers, numNodes, tmpOutput);
    }
  }

  /**
   * A Map Reduce job that verifies that the linked lists generated by
   * {@link Generator} do not have any holes.
   */
  static class Verify extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(Verify.class);
    private static final VLongWritable DEF = new VLongWritable(-1);

    private Job job;

    public static class VerifyMapper extends TableMapper<LongWritable, VLongWritable> {
      private LongWritable row = new LongWritable();
      private LongWritable ref = new LongWritable();
      private VLongWritable vrow = new VLongWritable();

      @Override
      protected void map(ImmutableBytesWritable key, Result value, Context context)
          throws IOException ,InterruptedException {
        row.set(Bytes.toLong(key.get()));
        context.write(row, DEF);

        long prev = Bytes.toLong(value.getValue(FAMILY_NAME, COLUMN_PREV));
        if (prev >= 0) {
          ref.set(prev);
          vrow.set(Bytes.toLong(key.get()));
          context.write(ref, vrow);
        }
      }
    }

    public static enum Counts {
      UNREFERENCED, UNDEFINED, REFERENCED, CORRUPT
    }

    public static class VerifyReducer extends Reducer<LongWritable,VLongWritable,Text,Text> {
      private ArrayList<Long> refs = new ArrayList<Long>();

      public void reduce(LongWritable key, Iterable<VLongWritable> values, Context context)
          throws IOException, InterruptedException {

        int defCount = 0;

        refs.clear();
        for (VLongWritable type : values) {
          if (type.get() == -1) {
            defCount++;
          } else {
            refs.add(type.get());
          }
        }

        // TODO check for more than one def, should not happen

        if (defCount == 0 && refs.size() > 0) {
          // this is bad, found a node that is referenced but not defined. It must have been
          //lost, emit some info about this node for debugging purposes.

          StringBuilder sb = new StringBuilder();
          String comma = "";
          for (Long ref : refs) {
            sb.append(comma);
            comma = ",";
            sb.append(String.format("%016x", ref));
          }

          context.write(new Text(String.format("%016x", key.get())), new Text(sb.toString()));
          context.getCounter(Counts.UNDEFINED).increment(1);

        } else if (defCount > 0 && refs.size() == 0) {
          // node is defined but not referenced
          context.getCounter(Counts.UNREFERENCED).increment(1);
        } else {
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

      Scan scan = new Scan();
      scan.addColumn(FAMILY_NAME, COLUMN_PREV);
      scan.setCaching(10000);
      scan.setCacheBlocks(false);

      TableMapReduceUtil.initTableMapperJob(getTableName(getConf()), scan,
          VerifyMapper.class, LongWritable.class, VLongWritable.class, job);

      job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);

      job.setReducerClass(VerifyReducer.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      TextOutputFormat.setOutputPath(job, outputDir);

      boolean success = job.waitForCompletion(true);

      return success ? 0 : 1;
    }

    public boolean verify(long expectedReferenced) throws Exception {
      if (job == null) {
        throw new IllegalStateException("You should call run() first");
      }

      Counters counters = job.getCounters();

      Counter referenced = counters.findCounter(Counts.REFERENCED);
      Counter unreferenced = counters.findCounter(Counts.UNREFERENCED);
      Counter undefined = counters.findCounter(Counts.UNDEFINED);

      boolean success = true;
      //assert
      if (expectedReferenced != referenced.getValue()) {
        LOG.error("Expected referenced count does not match with actual referenced count. " +
            "expected referenced=" + expectedReferenced + " ,actual=" + referenced.getValue());
        success = false;
      }

      if (unreferenced.getValue() > 0) {
        LOG.error("Unreferenced nodes were not expected. Unreferenced count=" + unreferenced.getValue());
        success = false;
      }

      if (undefined.getValue() > 0) {
        LOG.error("Found an undefined node. Undefined count=" + undefined.getValue());
        success = false;
      }

      return success;
    }
  }

  /**
   * Executes Generate and Verify in a loop. Data is not cleaned between runs, so each iteration
   * adds more data.
   */
  private static class Loop extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(Loop.class);

    protected void runGenerator(int numMappers, long numNodes, String outputDir) throws Exception {
      Path outputPath = new Path(outputDir);
      UUID uuid = UUID.randomUUID(); //create a random UUID.
      Path generatorOutput = new Path(outputPath, uuid.toString());

      Generator generator = new Generator();
      generator.setConf(getConf());
      int retCode = generator.run(numMappers, numNodes, generatorOutput);

      if (retCode > 0) {
        throw new RuntimeException("Generator failed with return code: " + retCode);
      }
    }

    protected void runVerify(String outputDir, int numReducers, long expectedNumNodes) throws Exception {
      Path outputPath = new Path(outputDir);
      UUID uuid = UUID.randomUUID(); //create a random UUID.
      Path iterationOutput = new Path(outputPath, uuid.toString());

      Verify verify = new Verify();
      verify.setConf(getConf());
      int retCode = verify.run(iterationOutput, numReducers);
      if (retCode > 0) {
        throw new RuntimeException("Verify.run failed with return code: " + retCode);
      }

      boolean verifySuccess = verify.verify(expectedNumNodes);
      if (!verifySuccess) {
        throw new RuntimeException("Verify.verify failed");
      }

      LOG.info("Verify finished with succees. Total nodes=" + expectedNumNodes);
    }

    @Override
    public int run(String[] args) throws Exception {
      if (args.length < 5) {
        System.err.println("Usage: Loop <num iterations> <num mappers> <num nodes per mapper> <output dir> <num reducers>");
        return 1;
      }

      LOG.info("Running Loop with args:" + Arrays.deepToString(args));

      int numIterations = Integer.parseInt(args[0]);
      int numMappers = Integer.parseInt(args[1]);
      long numNodes = Long.parseLong(args[2]);
      String outputDir = args[3];
      int numReducers = Integer.parseInt(args[4]);

      long expectedNumNodes = 0;

      if (numIterations < 0) {
        numIterations = Integer.MAX_VALUE; //run indefinitely (kind of)
      }

      for (int i=0; i < numIterations; i++) {
        LOG.info("Starting iteration = " + i);
        runGenerator(numMappers, numNodes, outputDir);
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
        scan.setStartRow(Bytes.toBytes(new BigInteger(cmd.getOptionValue("s"), 16).longValue()));

      if (cmd.hasOption("e"))
        scan.setStopRow(Bytes.toBytes(new BigInteger(cmd.getOptionValue("e"), 16).longValue()));

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
        System.out.printf("%016x:%016x:%012d:%s\n", node.key, node.prev, node.count, node.client);
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
    public int run(String[] args) throws Exception {
      if (args.length != 1) {
        System.out.println("Usage : " + Delete.class.getSimpleName() + " <node to delete>");
        return 0;
      }
      long val = new BigInteger(args[0], 16).longValue();

      org.apache.hadoop.hbase.client.Delete delete
        = new org.apache.hadoop.hbase.client.Delete(Bytes.toBytes(val));

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
    public int run(String[] args) throws IOException {
      Options options = new Options();
      options.addOption("n", "num", true, "number of queries");

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

      HTable table = new HTable(getConf(), getTableName(getConf()));

      Random rand = new Random();

      long numQueries = 0;

      while (numQueries < maxQueries) {
        CINode node = findStartNode(rand, table);
        numQueries++;
        while (node != null && node.prev >= 0 && numQueries < maxQueries) {
          long prev = node.prev;

          long t1 = System.currentTimeMillis();
          node = getNode(prev, table, node);
          long t2 = System.currentTimeMillis();
          System.out.printf("CQ %d %016x \n", t2 - t1, prev); //cold cache
          numQueries++;

          t1 = System.currentTimeMillis();
          node = getNode(prev, table, node);
          t2 = System.currentTimeMillis();
          System.out.printf("HQ %d %016x \n", t2 - t1, prev); //hot cache
          numQueries++;
        }
      }

      table.close();
      return 0;
    }

    private static CINode findStartNode(Random rand, HTable table) throws IOException {
      Scan scan = new Scan();
      scan.setStartRow(Bytes.toBytes(Math.abs(rand.nextLong())));
      scan.setBatch(1);
      scan.addColumn(FAMILY_NAME, COLUMN_PREV);

      long t1 = System.currentTimeMillis();
      ResultScanner scanner = table.getScanner(scan);
      Result result = scanner.next();
      long t2 = System.currentTimeMillis();
      scanner.close();

      if ( result != null) {
        CINode node = getCINode(result, new CINode());
        System.out.printf("FSR %d %016x\n", t2 - t1, node.key);
        return node;
      }

      System.out.println("FSR " + (t2 - t1));

      return null;
    }

    private CINode getNode(long row, HTable table, CINode node) throws IOException {
      Get get = new Get(Bytes.toBytes(row));
      get.addColumn(FAMILY_NAME, COLUMN_PREV);
      Result result = table.get(get);
      return getCINode(result, node);
    }
  }

  private static byte[] getTableName(Configuration conf) {
    return Bytes.toBytes(conf.get(TABLE_NAME_KEY, DEFAULT_TABLE_NAME));
  }

  private static CINode getCINode(Result result, CINode node) {
    node.key = Bytes.toLong(result.getRow());
    if (result.containsColumn(FAMILY_NAME, COLUMN_PREV)) {
      node.prev = Bytes.toLong(result.getValue(FAMILY_NAME, COLUMN_PREV));
    }
    if (result.containsColumn(FAMILY_NAME, COLUMN_COUNT)) {
      node.count = Bytes.toLong(result.getValue(FAMILY_NAME, COLUMN_COUNT));
    }
    if (result.containsColumn(FAMILY_NAME, COLUMN_CLIENT)) {
      node.client = Bytes.toString(result.getValue(FAMILY_NAME, COLUMN_CLIENT));
    }
    return node;
  }

  private IntegrationTestingUtility util;

  @Before
  public void setUp() throws Exception {
    util = getTestingUtil();
    util.initializeCluster(3);
    this.setConf(util.getConfiguration());
  }

  @After
  public void tearDown() throws Exception {
    util.restoreCluster();
  }

  @Test
  public void testContinuousIngest() throws IOException, Exception {
    //Loop <num iterations> <num mappers> <num nodes per mapper> <output dir> <num reducers>
    int ret = ToolRunner.run(getTestingUtil().getConfiguration(), new Loop(),
        new String[] {"1", "1", "2000000",
                     getTestDir("IntegrationTestBigLinkedList", "testContinuousIngest").toString(), "1"});
    org.junit.Assert.assertEquals(0, ret);
  }

  public Path getTestDir(String testName, String subdir) throws IOException {
    //HBaseTestingUtility.getDataTestDirOnTestFs() has not been backported.
    FileSystem fs = FileSystem.get(getConf());
    Path base = new Path(fs.getWorkingDirectory(), "test-data");
    String randomStr = UUID.randomUUID().toString();
    Path testDir = new Path(base, randomStr);
    fs.deleteOnExit(testDir);

    return new Path(new Path(testDir, testName), subdir);
  }

  private IntegrationTestingUtility getTestingUtil() {
    if (this.util == null) {
      if (getConf() == null) {
        this.util = new IntegrationTestingUtility();
      } else {
        this.util = new IntegrationTestingUtility(getConf());
      }
    }
    return util;
  }

  private int printUsage() {
    System.err.println("Usage: " + this.getClass().getSimpleName() + " COMMAND [COMMAND options]");
    System.err.println("  where COMMAND is one of:");
    System.err.println("");
    System.err.println("  Generator                  A map only job that generates data.");
    System.err.println("  Verify                     A map reduce job that looks for holes");
    System.err.println("                             Look at the counts after running");
    System.err.println("                             REFERENCED and UNREFERENCED are ok");
    System.err.println("                             any UNDEFINED counts are bad. Do not");
    System.err.println("                             run at the same time as the Generator.");
    System.err.println("  Walker                     A standalong program that starts ");
    System.err.println("                             following a linked list and emits");
    System.err.println("                             timing info.");
    System.err.println("  Print                      A standalone program that prints nodes");
    System.err.println("                             in the linked list.");
    System.err.println("  Delete                     A standalone program that deletes a·");
    System.err.println("                             single node.");
    System.err.println("  Loop                       A program to Loop through Generator and");
    System.err.println("                             Verify steps");
    System.err.println("\t  ");
    return 1;
  }

  @Override
  public int run(String[] args) throws Exception {
    //get the class, run with the conf
    if (args.length < 1) {
      return printUsage();
    }
    Tool tool = null;
    if (args[0].equals("Generator")) {
      tool = new Generator();
    } else if (args[0].equals("Verify")) {
      tool = new Verify();
    } else if (args[0].equals("Loop")) {
      tool = new Loop();
    } else if (args[0].equals("Walker")) {
      tool = new Walker();
    } else if (args[0].equals("Print")) {
      tool = new Print();
    } else if (args[0].equals("Delete")) {
      tool = new Delete();
    } else {
      return printUsage();
    }

    args = Arrays.copyOfRange(args, 1, args.length);
    return ToolRunner.run(getConf(), tool, args);
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new IntegrationTestBigLinkedList(), args);
    System.exit(ret);
  }
}
