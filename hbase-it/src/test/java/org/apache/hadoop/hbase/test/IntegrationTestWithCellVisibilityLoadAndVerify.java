/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerCallable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableRecordReaderImpl;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.security.visibility.VisibilityClient;
import org.apache.hadoop.hbase.security.visibility.VisibilityController;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.junit.experimental.categories.Category;

/**
 * A large test which loads a lot of data with cell visibility, and verifies the data. Test adds 2
 * users with different sets of visibility labels authenticated for them. Every row (so cells in
 * that) added with visibility expressions. In load step, 200 map tasks are launched, which in turn
 * write loadmapper.num_to_write (default 100K) rows to an hbase table. Rows are written in blocks,
 * for a total of 100 blocks.
 * 
 * Verify step scans the table as both users with Authorizations. This step asserts that user can
 * see only those rows (and so cells) with visibility for which they have label auth.
 * 
 * This class can be run as a unit test, as an integration test, or from the command line.
 * 
 * Originally taken from Apache Bigtop.
 * Issue user names as comma seperated list.
 *./hbase IntegrationTestWithCellVisibilityLoadAndVerify -u usera,userb
 */
@Category(IntegrationTests.class)
public class IntegrationTestWithCellVisibilityLoadAndVerify extends IntegrationTestLoadAndVerify {
  private static final String ERROR_STR = 
      "Two user names are to be specified seperated by a ',' like 'usera,userb'";
  private static final char NOT = '!';
  private static final char OR = '|';
  private static final char AND = '&';
  private static final String TEST_NAME = "IntegrationTestCellVisibilityLoadAndVerify";
  private static final String CONFIDENTIAL = "confidential";
  private static final String TOPSECRET = "topsecret";
  private static final String SECRET = "secret";
  private static final String PUBLIC = "public";
  private static final String PRIVATE = "private";
  private static final String[] LABELS = { CONFIDENTIAL, TOPSECRET, SECRET, PRIVATE, PUBLIC };
  private static final String[] VISIBILITY_EXPS = { CONFIDENTIAL + AND + TOPSECRET + AND + PRIVATE,
      CONFIDENTIAL + OR + TOPSECRET, PUBLIC,
      '(' + SECRET + OR + PRIVATE + ')' + AND + NOT + CONFIDENTIAL };
  private static final int VISIBILITY_EXPS_COUNT = VISIBILITY_EXPS.length;
  private static final byte[] TEST_FAMILY = Bytes.toBytes("f1");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("q1");
  private static final String NUM_TO_WRITE_KEY = "loadmapper.num_to_write";
  private static final long NUM_TO_WRITE_DEFAULT = 100 * 1000;
  private static final int SCANNER_CACHING = 500;
  private static String USER_OPT = "users";
  private static String userNames = "user1,user2";

  private long numRowsLoadedWithExp1, numRowsLoadedWithExp2, numRowsLoadWithExp3,
      numRowsLoadWithExp4;
  private long numRowsReadWithExp1, numRowsReadWithExp2, numRowsReadWithExp3, numRowsReadWithExp4;

  private static User USER1, USER2;

  private enum Counters {
    ROWS_VIS_EXP_1, ROWS_VIS_EXP_2, ROWS_VIS_EXP_3, ROWS_VIS_EXP_4;
  }

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(null);
    Configuration conf = util.getConfiguration();
    conf.setInt(HFile.FORMAT_VERSION_KEY, 3);
    conf.set("hbase.coprocessor.master.classes", VisibilityController.class.getName());
    conf.set("hbase.coprocessor.region.classes", VisibilityController.class.getName());
    conf.set("hbase.superuser", User.getCurrent().getName());
    conf.setBoolean("dfs.permissions", false);
    super.setUpCluster();
    String[] users = userNames.split(",");
    if (users.length != 2) {
      System.err.println(ERROR_STR);
      throw new IOException(ERROR_STR);
    }
    System.out.println(userNames + " "+users[0]+ " "+users[1]);
    USER1 = User.createUserForTesting(conf, users[0], new String[] {});
    USER2 = User.createUserForTesting(conf, users[1], new String[] {});
    addLabelsAndAuths();
  }

  @Override
  protected void addOptions() {
    super.addOptions();
    addOptWithArg("u", USER_OPT, "User names to be passed");
  }

  private void addLabelsAndAuths() throws Exception {
    try {
      VisibilityClient.addLabels(util.getConfiguration(), LABELS);
      VisibilityClient.setAuths(util.getConfiguration(), new String[] { CONFIDENTIAL, TOPSECRET,
          SECRET, PRIVATE }, USER1.getName());
      VisibilityClient.setAuths(util.getConfiguration(), new String[] { PUBLIC },
          USER2.getName());
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  public static class LoadWithCellVisibilityMapper extends LoadMapper {
    private Counter rowsExp1, rowsExp2, rowsExp3, rowsexp4;

    @Override
    public void setup(Context context) throws IOException {
      super.setup(context);
      rowsExp1 = context.getCounter(Counters.ROWS_VIS_EXP_1);
      rowsExp2 = context.getCounter(Counters.ROWS_VIS_EXP_2);
      rowsExp3 = context.getCounter(Counters.ROWS_VIS_EXP_3);
      rowsexp4 = context.getCounter(Counters.ROWS_VIS_EXP_4);
    }

    @Override
    protected void map(NullWritable key, NullWritable value, Context context) throws IOException,
        InterruptedException {
      String suffix = "/" + shortTaskId;
      int BLOCK_SIZE = (int) (recordsToWrite / 100);
      for (long i = 0; i < recordsToWrite;) {
        for (long idx = 0; idx < BLOCK_SIZE && i < recordsToWrite; idx++, i++) {
          int expIdx = rand.nextInt(BLOCK_SIZE) % VISIBILITY_EXPS_COUNT;
          String exp = VISIBILITY_EXPS[expIdx];
          byte[] row = Bytes.add(Bytes.toBytes(i), Bytes.toBytes(suffix), Bytes.toBytes(exp));
          Put p = new Put(row);
          p.add(TEST_FAMILY, TEST_QUALIFIER, HConstants.EMPTY_BYTE_ARRAY);
          p.setCellVisibility(new CellVisibility(exp));
          getCounter(expIdx).increment(1);
          table.put(p);

          if (i % 100 == 0) {
            context.setStatus("Written " + i + "/" + recordsToWrite + " records");
            context.progress();
          }
        }
        // End of block, flush all of them before we start writing anything
        // pointing to these!
        table.flushCommits();
      }
    }

    private Counter getCounter(int idx) {
      switch (idx) {
      case 0:
        return rowsExp1;
      case 1:
        return rowsExp2;
      case 2:
        return rowsExp3;
      case 3:
        return rowsexp4;
      default:
        return null;
      }
    }
  }

  public static class VerifyMapper extends TableMapper<BytesWritable, BytesWritable> {
    private Counter rowsExp1, rowsExp2, rowsExp3, rowsExp4;

    @Override
    public void setup(Context context) throws IOException {
      rowsExp1 = context.getCounter(Counters.ROWS_VIS_EXP_1);
      rowsExp2 = context.getCounter(Counters.ROWS_VIS_EXP_2);
      rowsExp3 = context.getCounter(Counters.ROWS_VIS_EXP_3);
      rowsExp4 = context.getCounter(Counters.ROWS_VIS_EXP_4);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException, InterruptedException {
      byte[] row = value.getRow();
      Counter c = getCounter(row);
      c.increment(1);
    }

    private Counter getCounter(byte[] row) {
      Counter c = null;
      if (Bytes.indexOf(row, Bytes.toBytes(VISIBILITY_EXPS[0])) != -1) {
        c = rowsExp1;
      } else if (Bytes.indexOf(row, Bytes.toBytes(VISIBILITY_EXPS[1])) != -1) {
        c = rowsExp2;
      } else if (Bytes.indexOf(row, Bytes.toBytes(VISIBILITY_EXPS[2])) != -1) {
        c = rowsExp3;
      } else if (Bytes.indexOf(row, Bytes.toBytes(VISIBILITY_EXPS[3])) != -1) {
        c = rowsExp4;
      }
      return c;
    }
  }

  @Override
  protected Job doLoad(Configuration conf, HTableDescriptor htd) throws Exception {
    Job job = super.doLoad(conf, htd);
    this.numRowsLoadedWithExp1 = job.getCounters().findCounter(Counters.ROWS_VIS_EXP_1).getValue();
    this.numRowsLoadedWithExp2 = job.getCounters().findCounter(Counters.ROWS_VIS_EXP_2).getValue();
    this.numRowsLoadWithExp3 = job.getCounters().findCounter(Counters.ROWS_VIS_EXP_3).getValue();
    this.numRowsLoadWithExp4 = job.getCounters().findCounter(Counters.ROWS_VIS_EXP_4).getValue();
    System.out.println("Rows loaded with cell visibility " + VISIBILITY_EXPS[0] + " : "
        + this.numRowsLoadedWithExp1);
    System.out.println("Rows loaded with cell visibility " + VISIBILITY_EXPS[1] + " : "
        + this.numRowsLoadedWithExp2);
    System.out.println("Rows loaded with cell visibility " + VISIBILITY_EXPS[2] + " : "
        + this.numRowsLoadWithExp3);
    System.out.println("Rows loaded with cell visibility " + VISIBILITY_EXPS[3] + " : "
        + this.numRowsLoadWithExp4);
    return job;
  }

  protected void setMapperClass(Job job) {
    job.setMapperClass(LoadWithCellVisibilityMapper.class);
  }

  protected void doVerify(final Configuration conf, final HTableDescriptor htd) throws Exception {
    System.out.println(String.format("Verifying for auths %s, %s, %s, %s", CONFIDENTIAL, TOPSECRET,
        SECRET, PRIVATE));
    PrivilegedExceptionAction<Job> scanAction = new PrivilegedExceptionAction<Job>() {
      @Override
      public Job run() throws Exception {
        return doVerify(conf, htd, CONFIDENTIAL, TOPSECRET, SECRET, PRIVATE);
      }
    };
    Job job = USER1.runAs(scanAction);
    this.numRowsReadWithExp1 = job.getCounters().findCounter(Counters.ROWS_VIS_EXP_1).getValue();
    this.numRowsReadWithExp2 = job.getCounters().findCounter(Counters.ROWS_VIS_EXP_2).getValue();
    this.numRowsReadWithExp3 = job.getCounters().findCounter(Counters.ROWS_VIS_EXP_3).getValue();
    this.numRowsReadWithExp4 = job.getCounters().findCounter(Counters.ROWS_VIS_EXP_4).getValue();
    assertEquals(this.numRowsLoadedWithExp1, this.numRowsReadWithExp1);
    assertEquals(this.numRowsLoadedWithExp2, this.numRowsReadWithExp2);
    assertEquals(0, this.numRowsReadWithExp3);
    assertEquals(0, this.numRowsReadWithExp4);

    // PUBLIC label auth is not provided for user1 user.
    System.out.println(String.format("Verifying for auths %s, %s", PRIVATE, PUBLIC));
    scanAction = new PrivilegedExceptionAction<Job>() {
      @Override
      public Job run() throws Exception {
        return doVerify(conf, htd, PRIVATE, PUBLIC);
      }
    };
    job = USER1.runAs(scanAction);
    this.numRowsReadWithExp1 = job.getCounters().findCounter(Counters.ROWS_VIS_EXP_1).getValue();
    this.numRowsReadWithExp2 = job.getCounters().findCounter(Counters.ROWS_VIS_EXP_2).getValue();
    this.numRowsReadWithExp3 = job.getCounters().findCounter(Counters.ROWS_VIS_EXP_3).getValue();
    this.numRowsReadWithExp4 = job.getCounters().findCounter(Counters.ROWS_VIS_EXP_4).getValue();
    assertEquals(0, this.numRowsReadWithExp1);
    assertEquals(0, this.numRowsReadWithExp2);
    assertEquals(0, this.numRowsReadWithExp3);
    assertEquals(this.numRowsLoadWithExp4, this.numRowsReadWithExp4);

    // Normal user only having PUBLIC label auth and can view only those cells.
    System.out.println(String.format("Verifying for auths %s, %s", PRIVATE, PUBLIC));
    scanAction = new PrivilegedExceptionAction<Job>() {
      @Override
      public Job run() throws Exception {
        return doVerify(conf, htd, PRIVATE, PUBLIC);
      }
    };
    job = USER2.runAs(scanAction);
    this.numRowsReadWithExp1 = job.getCounters().findCounter(Counters.ROWS_VIS_EXP_1).getValue();
    this.numRowsReadWithExp2 = job.getCounters().findCounter(Counters.ROWS_VIS_EXP_2).getValue();
    this.numRowsReadWithExp3 = job.getCounters().findCounter(Counters.ROWS_VIS_EXP_3).getValue();
    this.numRowsReadWithExp4 = job.getCounters().findCounter(Counters.ROWS_VIS_EXP_4).getValue();
    assertEquals(0, this.numRowsReadWithExp1);
    assertEquals(0, this.numRowsReadWithExp2);
    assertEquals(this.numRowsLoadWithExp3, this.numRowsReadWithExp3);
    assertEquals(0, this.numRowsReadWithExp4);
  }

  private Job doVerify(Configuration conf, HTableDescriptor htd, String... auths)
      throws IOException, InterruptedException, ClassNotFoundException {
    Path outputDir = getTestDir(TEST_NAME, "verify-output");
    Job job = new Job(conf);
    job.setJarByClass(this.getClass());
    job.setJobName(TEST_NAME + " Verification for " + htd.getTableName());
    setJobScannerConf(job);
    Scan scan = new Scan();
    scan.setAuthorizations(new Authorizations(auths));
    TableMapReduceUtil.initTableMapperJob(htd.getTableName().getNameAsString(), scan,
        VerifyMapper.class, NullWritable.class, NullWritable.class, job);
    TableMapReduceUtil.addDependencyJars(job.getConfiguration(), AbstractHBaseTool.class);
    int scannerCaching = conf.getInt("verify.scannercaching", SCANNER_CACHING);
    TableMapReduceUtil.setScannerCaching(job, scannerCaching);
    job.setNumReduceTasks(0);
    FileOutputFormat.setOutputPath(job, outputDir);
    assertTrue(job.waitForCompletion(true));
    return job;
  }

  private static void setJobScannerConf(Job job) {
    job.getConfiguration().setBoolean(ScannerCallable.LOG_SCANNER_ACTIVITY, true);
    long lpr = job.getConfiguration().getLong(NUM_TO_WRITE_KEY, NUM_TO_WRITE_DEFAULT) / 100;
    job.getConfiguration().setInt(TableRecordReaderImpl.LOG_PER_ROW_COUNT, (int) lpr);
  }

  public void usage() {
    System.err.println(this.getClass().getSimpleName() + " -u usera,userb [-Doptions]");
    System.err.println("  Loads a table with cell visibilities and verifies with Authorizations");
    System.err.println("Options");
    System.err
        .println("  -Dloadmapper.table=<name>        Table to write/verify (default autogen)");
    System.err.println("  -Dloadmapper.num_to_write=<n>    "
        + "Number of rows per mapper (default 100,000 per mapper)");
    System.err.println("  -Dloadmapper.numPresplits=<n>    "
        + "Number of presplit regions to start with (default 40)");
    System.err
        .println("  -Dloadmapper.map.tasks=<n>       Number of map tasks for load (default 200)");
    System.err.println("  -Dverify.scannercaching=<n>      "
        + "Number hbase scanner caching rows to read (default 50)");
  }

  public int runTestFromCommandLine() throws Exception {
    IntegrationTestingUtility.setUseDistributedCluster(getConf());
    int numPresplits = getConf().getInt("loadmapper.numPresplits", 5);
    // create HTableDescriptor for specified table
    String table = getTablename();
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(table));
    htd.addFamily(new HColumnDescriptor(TEST_FAMILY));

    HBaseAdmin admin = new HBaseAdmin(getConf());
    try {
      admin.createTable(htd, Bytes.toBytes(0L), Bytes.toBytes(-1L), numPresplits);
    } finally {
      admin.close();
    }
    doLoad(getConf(), htd);
    doVerify(getConf(), htd);
    getTestingUtil(getConf()).deleteTable(htd.getName());
    return 0;
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    List args = cmd.getArgList();
    if (args.size() > 0) {
      usage();
      throw new RuntimeException("No args expected.");
    }
    // We always want loadAndVerify action
    args.add("loadAndVerify");
    if (cmd.hasOption(USER_OPT)) {
      userNames = cmd.getOptionValue(USER_OPT);
    }
    super.processOptions(cmd);
  }

  public static void main(String argv[]) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestWithCellVisibilityLoadAndVerify(), argv);
    System.exit(ret);
  }
}
