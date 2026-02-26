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

import static org.apache.hadoop.hbase.IntegrationTestingUtility.DEFAULT_REGIONS_PER_SERVER;
import static org.apache.hadoop.hbase.IntegrationTestingUtility.PRESPLIT_TEST_TABLE;
import static org.apache.hadoop.hbase.IntegrationTestingUtility.PRESPLIT_TEST_TABLE_KEY;
import static org.apache.hadoop.hbase.IntegrationTestingUtility.REGIONS_PER_SERVER_KEY;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.IntegrationTestBase;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ScanResultConsumer;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.test.util.CRC64;
import org.apache.hadoop.hbase.test.util.warc.WARCInputFormat;
import org.apache.hadoop.hbase.test.util.warc.WARCRecord;
import org.apache.hadoop.hbase.test.util.warc.WARCWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExitHandler;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Splitter;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

/**
 * This integration test loads successful resource retrieval records from the Common Crawl
 * (https://commoncrawl.org/) public dataset into an HBase table and writes records that can be used
 * to later verify the presence and integrity of those records.
 * <p>
 * Run like: <blockquote> ./bin/hbase org.apache.hadoop.hbase.test.IntegrationTestLoadCommonCrawl
 * \<br>
 * &nbsp;&nbsp; -Dfs.s3n.awsAccessKeyId=&lt;AWS access key&gt; \<br>
 * &nbsp;&nbsp; -Dfs.s3n.awsSecretAccessKey=&lt;AWS secret key&gt; \<br>
 * &nbsp;&nbsp; /path/to/test-CC-MAIN-2021-10-warc.paths.gz \<br>
 * &nbsp;&nbsp; /path/to/tmp/warc-loader-output </blockquote>
 * <p>
 * Access to the Common Crawl dataset in S3 is made available to anyone by Amazon AWS, but Hadoop's
 * S3N filesystem still requires valid access credentials to initialize.
 * <p>
 * The input path can either specify a directory or a file. The file may optionally be compressed
 * with gzip. If a directory, the loader expects the directory to contain one or more WARC files
 * from the Common Crawl dataset. If a file, the loader expects a list of Hadoop S3N URIs which
 * point to S3 locations for one or more WARC files from the Common Crawl dataset, one URI per line.
 * Lines should be terminated with the UNIX line terminator.
 * <p>
 * Included in hbase-it/src/test/resources/CC-MAIN-2021-10-warc.paths.gz is a list of all WARC files
 * comprising the Q1 2021 crawl archive. There are 64,000 WARC files in this data set, each
 * containing ~1GB of gzipped data. The WARC files contain several record types, such as metadata,
 * request, and response, but we only load the response record types. If the HBase table schema does
 * not specify compression (by default) there is roughly a 10x expansion. Loading the full crawl
 * archive results in a table approximately 640 TB in size.
 * <p>
 * The loader can optionally drive read load during ingest by incrementing counters for each URL
 * discovered in content. Add <tt>-DIntegrationTestLoadCommonCrawl.increments=true</tt> to the
 * command line to enable.
 * <p>
 * You can also split the Loader and Verify stages:
 * <p>
 * Load with: <blockquote> ./bin/hbase
 * 'org.apache.hadoop.hbase.test.IntegrationTestLoadCommonCrawl$Loader' \<br>
 * &nbsp;&nbsp; -files /path/to/hadoop-aws.jar \<br>
 * &nbsp;&nbsp; -Dfs.s3n.awsAccessKeyId=&lt;AWS access key&gt; \<br>
 * &nbsp;&nbsp; -Dfs.s3n.awsSecretAccessKey=&lt;AWS secret key&gt; \<br>
 * &nbsp;&nbsp; /path/to/test-CC-MAIN-2021-10-warc.paths.gz \<br>
 * &nbsp;&nbsp; /path/to/tmp/warc-loader-output </blockquote>
 * <p>
 * Note: The hadoop-aws jar will be needed at runtime to instantiate the S3N filesystem. Use the
 * <tt>-files</tt> ToolRunner argument to add it.
 * <p>
 * Verify with: <blockquote> ./bin/hbase
 * 'org.apache.hadoop.hbase.test.IntegrationTestLoadCommonCrawl$Verify' \<br>
 * &nbsp;&nbsp; /path/to/tmp/warc-loader-output </blockquote>
 * <p>
 */
public class IntegrationTestLoadCommonCrawl extends IntegrationTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestLoadCommonCrawl.class);

  static final String TABLE_NAME_KEY = "IntegrationTestLoadCommonCrawl.table";
  static final String DEFAULT_TABLE_NAME = "IntegrationTestLoadCommonCrawl";

  static final String INCREMENTS_NAME_KEY = "IntegrationTestLoadCommonCrawl.increments";
  static final boolean DEFAULT_INCREMENTS = false;

  static final int MAX_INFLIGHT = 1000;
  static final int INFLIGHT_PAUSE_MS = 100;

  static final byte[] CONTENT_FAMILY_NAME = Bytes.toBytes("c");
  static final byte[] INFO_FAMILY_NAME = Bytes.toBytes("i");
  static final byte[] URL_FAMILY_NAME = Bytes.toBytes("u");
  static final byte[] SEP = Bytes.toBytes(":");
  static final byte[] CONTENT_QUALIFIER = HConstants.EMPTY_BYTE_ARRAY;
  static final byte[] CONTENT_LENGTH_QUALIFIER = Bytes.toBytes("l");
  static final byte[] CONTENT_TYPE_QUALIFIER = Bytes.toBytes("t");
  static final byte[] CRC_QUALIFIER = Bytes.toBytes("c");
  static final byte[] DATE_QUALIFIER = Bytes.toBytes("d");
  static final byte[] IP_ADDRESS_QUALIFIER = Bytes.toBytes("a");
  static final byte[] TARGET_URI_QUALIFIER = Bytes.toBytes("u");
  static final byte[] REF_QUALIFIER = Bytes.toBytes("ref");

  public static enum Counts {
    REFERENCED,
    UNREFERENCED,
    CORRUPT,
    RPC_BYTES_WRITTEN,
    RPC_TIME_MS,
  }

  protected Path warcFileInputDir = null;
  protected Path outputDir = null;
  protected String[] args;

  protected int runLoader(final Path warcFileInputDir, final Path outputDir) throws Exception {
    Loader loader = new Loader();
    loader.setConf(conf);
    return loader.run(warcFileInputDir, outputDir);
  }

  protected int runVerify(final Path inputDir) throws Exception {
    Verify verify = new Verify();
    verify.setConf(conf);
    return verify.run(inputDir);
  }

  @Override
  public int run(String[] args) {
    if (args.length > 0) {
      warcFileInputDir = new Path(args[0]);
      if (args.length > 1) {
        outputDir = new Path(args[1]);
      }
    }
    try {
      if (warcFileInputDir == null) {
        throw new IllegalArgumentException("WARC input file or directory not specified");
      }
      if (outputDir == null) {
        throw new IllegalArgumentException("Output directory not specified");
      }
      int res = runLoader(warcFileInputDir, outputDir);
      if (res != 0) {
        LOG.error("Loader failed");
        return -1;
      }
      return runVerify(outputDir);
    } catch (Exception e) {
      LOG.error("Tool failed with exception", e);
      return -1;
    }
  }

  @Override
  protected void processOptions(final CommandLine cmd) {
    processBaseOptions(cmd);
    args = cmd.getArgs();
  }

  @Override
  public void setUpCluster() throws Exception {
    util = getTestingUtil(getConf());
    boolean isDistributed = util.isDistributedCluster();
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

  static TableName getTablename(final Configuration c) {
    return TableName.valueOf(c.get(TABLE_NAME_KEY, DEFAULT_TABLE_NAME));
  }

  @Override
  public TableName getTablename() {
    return getTablename(getConf());
  }

  @Override
  protected Set<String> getColumnFamilies() {
    Set<String> families = new HashSet<>();
    families.add(Bytes.toString(CONTENT_FAMILY_NAME));
    families.add(Bytes.toString(INFO_FAMILY_NAME));
    families.add(Bytes.toString(URL_FAMILY_NAME));
    return families;
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    return ToolRunner.run(getConf(), this, args);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestLoadCommonCrawl(), args);
    ExitHandler.getInstance().exit(ret);
  }

  public static class HBaseKeyWritable implements Writable {

    private byte[] row;
    private int rowOffset;
    private int rowLength;
    private byte[] family;
    private int familyOffset;
    private int familyLength;
    private byte[] qualifier;
    private int qualifierOffset;
    private int qualifierLength;
    private long ts;

    public HBaseKeyWritable() {
    }

    public HBaseKeyWritable(byte[] row, int rowOffset, int rowLength, byte[] family,
      int familyOffset, int familyLength, byte[] qualifier, int qualifierOffset,
      int qualifierLength, long ts) {
      this.row = row;
      this.rowOffset = rowOffset;
      this.rowLength = rowLength;
      this.family = family;
      this.familyOffset = familyOffset;
      this.familyLength = familyLength;
      this.qualifier = qualifier;
      this.qualifierOffset = qualifierOffset;
      this.qualifierLength = qualifierLength;
      this.ts = ts;
    }

    public HBaseKeyWritable(byte[] row, byte[] family, byte[] qualifier, long ts) {
      this(row, 0, row.length, family, 0, family.length, qualifier, 0,
        qualifier != null ? qualifier.length : 0, ts);
    }

    public HBaseKeyWritable(byte[] row, byte[] family, byte[] qualifier) {
      this(row, family, qualifier, Long.MAX_VALUE);
    }

    public HBaseKeyWritable(byte[] row, byte[] family, long ts) {
      this(row, family, HConstants.EMPTY_BYTE_ARRAY, ts);
    }

    public HBaseKeyWritable(byte[] row, byte[] family) {
      this(row, family, Long.MAX_VALUE);
    }

    public HBaseKeyWritable(Cell cell) {
      this(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), cell.getFamilyArray(),
        cell.getFamilyOffset(), cell.getFamilyLength(), cell.getQualifierArray(),
        cell.getQualifierOffset(), cell.getQualifierLength(), cell.getTimestamp());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.row = Bytes.toBytes(in.readUTF());
      this.rowOffset = 0;
      this.rowLength = row.length;
      this.family = Bytes.toBytes(in.readUTF());
      this.familyOffset = 0;
      this.familyLength = family.length;
      this.qualifier = Bytes.toBytes(in.readUTF());
      this.qualifierOffset = 0;
      this.qualifierLength = qualifier.length;
      this.ts = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(new String(row, rowOffset, rowLength, StandardCharsets.UTF_8));
      out.writeUTF(new String(family, familyOffset, familyLength, StandardCharsets.UTF_8));
      if (qualifier != null) {
        out.writeUTF(
          new String(qualifier, qualifierOffset, qualifierLength, StandardCharsets.UTF_8));
      } else {
        out.writeUTF("");
      }
      out.writeLong(ts);
    }

    public byte[] getRowArray() {
      return row;
    }

    public void setRow(byte[] row) {
      this.row = row;
    }

    public int getRowOffset() {
      return rowOffset;
    }

    public void setRowOffset(int rowOffset) {
      this.rowOffset = rowOffset;
    }

    public int getRowLength() {
      return rowLength;
    }

    public void setRowLength(int rowLength) {
      this.rowLength = rowLength;
    }

    public byte[] getFamilyArray() {
      return family;
    }

    public void setFamily(byte[] family) {
      this.family = family;
    }

    public int getFamilyOffset() {
      return familyOffset;
    }

    public void setFamilyOffset(int familyOffset) {
      this.familyOffset = familyOffset;
    }

    public int getFamilyLength() {
      return familyLength;
    }

    public void setFamilyLength(int familyLength) {
      this.familyLength = familyLength;
    }

    public byte[] getQualifierArray() {
      return qualifier;
    }

    public void setQualifier(byte[] qualifier) {
      this.qualifier = qualifier;
    }

    public int getQualifierOffset() {
      return qualifierOffset;
    }

    public void setQualifierOffset(int qualifierOffset) {
      this.qualifierOffset = qualifierOffset;
    }

    public int getQualifierLength() {
      return qualifierLength;
    }

    public void setQualifierLength(int qualifierLength) {
      this.qualifierLength = qualifierLength;
    }

    public long getTimestamp() {
      return ts;
    }

    public void setTimestamp(long ts) {
      this.ts = ts;
    }
  }

  public static class Loader extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(Loader.class);
    private static final String USAGE = "Loader <warInputDir | warFileList> <outputDir>";

    void createSchema(final TableName tableName) throws IOException {

      try (Connection conn = ConnectionFactory.createConnection(getConf());
        Admin admin = conn.getAdmin()) {
        if (!admin.tableExists(tableName)) {

          ColumnFamilyDescriptorBuilder contentFamilyBuilder =
            ColumnFamilyDescriptorBuilder.newBuilder(CONTENT_FAMILY_NAME).setMaxVersions(1000)
              .setDataBlockEncoding(DataBlockEncoding.NONE).setBloomFilterType(BloomType.ROW);

          ColumnFamilyDescriptorBuilder infoFamilyBuilder =
            ColumnFamilyDescriptorBuilder.newBuilder(INFO_FAMILY_NAME).setMaxVersions(1000)
              .setDataBlockEncoding(DataBlockEncoding.ROW_INDEX_V1)
              .setBloomFilterType(BloomType.ROWCOL).setBlocksize(8 * 1024);

          ColumnFamilyDescriptorBuilder urlFamilyBuilder =
            ColumnFamilyDescriptorBuilder.newBuilder(URL_FAMILY_NAME).setMaxVersions(1000)
              .setDataBlockEncoding(DataBlockEncoding.ROW_INDEX_V1)
              .setBloomFilterType(BloomType.ROWCOL).setBlocksize(8 * 1024);

          Set<ColumnFamilyDescriptor> families = new HashSet<>();
          families.add(contentFamilyBuilder.build());
          families.add(infoFamilyBuilder.build());
          families.add(urlFamilyBuilder.build());

          TableDescriptor tableDescriptor =
            TableDescriptorBuilder.newBuilder(tableName).setColumnFamilies(families).build();

          if (getConf().getBoolean(PRESPLIT_TEST_TABLE_KEY, PRESPLIT_TEST_TABLE)) {
            int numberOfServers = admin.getRegionServers().size();
            if (numberOfServers == 0) {
              throw new IllegalStateException("No live regionservers");
            }
            int regionsPerServer =
              getConf().getInt(REGIONS_PER_SERVER_KEY, DEFAULT_REGIONS_PER_SERVER);
            int totalNumberOfRegions = numberOfServers * regionsPerServer;
            LOG.info("Creating test table: " + tableDescriptor);
            LOG.info("Number of live regionservers: " + numberOfServers + ", "
              + "pre-splitting table into " + totalNumberOfRegions + " regions "
              + "(default regions per server: " + regionsPerServer + ")");
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

    int run(final Path warcFileInput, final Path outputDir)
      throws IOException, ClassNotFoundException, InterruptedException {

      createSchema(getTablename(getConf()));

      final Job job = Job.getInstance(getConf());
      job.setJobName(Loader.class.getName());
      job.setNumReduceTasks(0);
      job.setJarByClass(getClass());
      job.setMapperClass(LoaderMapper.class);
      job.setInputFormatClass(WARCInputFormat.class);
      final FileSystem fs = FileSystem.get(warcFileInput.toUri(), getConf());
      if (fs.getFileStatus(warcFileInput).isDirectory()) {
        LOG.info("Using directory as WARC input path: " + warcFileInput);
        FileInputFormat.setInputPaths(job, warcFileInput);
      } else if (warcFileInput.toUri().getScheme().equals("file")) {
        LOG.info("Getting WARC input paths from file: " + warcFileInput);
        final List<Path> paths = new ArrayList<Path>();
        try (FSDataInputStream is = fs.open(warcFileInput)) {
          InputStreamReader reader;
          if (warcFileInput.getName().toLowerCase().endsWith(".gz")) {
            reader = new InputStreamReader(new GZIPInputStream(is), StandardCharsets.UTF_8);
          } else {
            reader = new InputStreamReader(is, StandardCharsets.UTF_8);
          }
          try (BufferedReader br = new BufferedReader(reader)) {
            String line;
            while ((line = br.readLine()) != null) {
              paths.add(new Path(line));
            }
          }
        }
        LOG.info("Read " + paths.size() + " WARC input paths from " + warcFileInput);
        FileInputFormat.setInputPaths(job, paths.toArray(new Path[paths.size()]));
      } else {
        FileInputFormat.setInputPaths(job, warcFileInput);
      }
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      SequenceFileOutputFormat.setOutputPath(job, outputDir);
      SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
      job.setOutputKeyClass(HBaseKeyWritable.class);
      job.setOutputValueClass(BytesWritable.class);
      TableMapReduceUtil.addDependencyJars(job);
      // Increase max attempts because S3 might throttle aggressively and ultimately fail a task
      job.getConfiguration().setInt("mapred.map.max.attempts", 100);
      job.getConfiguration().setInt("mapreduce.map.maxattempts", 100);

      boolean success = job.waitForCompletion(true);
      if (!success) {
        LOG.error("Failure during job " + job.getJobID());
      }

      final Counters counters = job.getCounters();
      for (Counts c : Counts.values()) {
        long value = counters.findCounter(c).getValue();
        if (value != 0) {
          LOG.info(c + ": " + value);
        }
      }

      return success ? 0 : 1;
    }

    @Override
    public int run(String[] args) throws Exception {
      if (args.length < 2) {
        System.err.println(USAGE);
        return 1;
      }
      try {
        Path warcFileInput = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        return run(warcFileInput, outputDir);
      } catch (NumberFormatException e) {
        System.err.println("Parsing loader arguments failed: " + e.getMessage());
        System.err.println(USAGE);
        return 1;
      }
    }

    public static void main(String[] args) throws Exception {
      ExitHandler.getInstance()
        .exit(ToolRunner.run(HBaseConfiguration.create(), new Loader(), args));
    }

    public static class LoaderMapper
      extends Mapper<LongWritable, WARCWritable, HBaseKeyWritable, BytesWritable> {

      protected AsyncConnection conn;
      protected AsyncTable<ScanResultConsumer> table;
      protected ExecutorService executor;
      protected AtomicLong inflight = new AtomicLong();
      protected boolean doIncrements;

      @Override
      protected void setup(final Context context) throws IOException, InterruptedException {
        executor = Executors.newWorkStealingPool(Runtime.getRuntime().availableProcessors());
        Configuration conf = context.getConfiguration();
        doIncrements = conf.getBoolean(INCREMENTS_NAME_KEY, DEFAULT_INCREMENTS);
        try {
          conn = ConnectionFactory.createAsyncConnection(conf).get();
          table = conn.getTable(getTablename(conf), executor);
        } catch (ExecutionException e) {
          throw new IOException(e);
        }
      }

      @Override
      protected void cleanup(final Context context) throws IOException, InterruptedException {

        while (inflight.get() != 0) {
          LOG.info("Operations in flight, waiting");
          Thread.sleep(INFLIGHT_PAUSE_MS);
        }

        // Shut down the executor
        executor.shutdown();
        if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
          LOG.warn("Pool did not shut down cleanly");
        }
        // Close the connection
        try {
          conn.close();
        } catch (Exception e) {
          LOG.warn("Exception closing Connection", e);
        }
      }

      @Override
      protected void map(final LongWritable key, final WARCWritable value, final Context output)
        throws IOException, InterruptedException {
        final WARCRecord.Header warcHeader = value.getRecord().getHeader();
        final String recordID = warcHeader.getRecordID();
        final String targetURI = warcHeader.getTargetURI();
        if (warcHeader.getRecordType().equals("response") && targetURI != null) {
          final String contentType = warcHeader.getField("WARC-Identified-Payload-Type");
          if (contentType != null) {
            // Make row key
            byte[] rowKey;
            try {
              rowKey = rowKeyFromTargetURI(targetURI);
            } catch (IllegalArgumentException e) {
              LOG.debug("Could not make a row key for record " + recordID + ", ignoring", e);
              return;
            } catch (URISyntaxException e) {
              LOG.warn(
                "Could not parse URI \"" + targetURI + "\" for record " + recordID + ", ignoring");
              return;
            }

            // Get the content and calculate the CRC64
            final byte[] content = value.getRecord().getContent();
            final CRC64 crc = new CRC64();
            crc.update(content);
            final long crc64 = crc.getValue();
            LOG.info("{}: content {} bytes, crc64={}", targetURI, content.length,
              Bytes.toHex(Bytes.toBytes(crc64)));

            // Store to HBase
            final long ts = getSequence();
            final Put put = new Put(rowKey);
            put.addColumn(CONTENT_FAMILY_NAME, CONTENT_QUALIFIER, ts, content);
            put.addColumn(INFO_FAMILY_NAME, CONTENT_LENGTH_QUALIFIER, ts,
              Bytes.toBytes(content.length));
            put.addColumn(INFO_FAMILY_NAME, CONTENT_TYPE_QUALIFIER, ts, Bytes.toBytes(contentType));
            put.addColumn(INFO_FAMILY_NAME, CRC_QUALIFIER, ts, Bytes.toBytes(crc64));
            put.addColumn(INFO_FAMILY_NAME, TARGET_URI_QUALIFIER, ts, Bytes.toBytes(targetURI));
            put.addColumn(INFO_FAMILY_NAME, DATE_QUALIFIER, ts,
              Bytes.toBytes(warcHeader.getDateString()));
            final String ipAddr = warcHeader.getField("WARC-IP-Address");
            if (ipAddr != null) {
              put.addColumn(INFO_FAMILY_NAME, IP_ADDRESS_QUALIFIER, ts, Bytes.toBytes(ipAddr));
            }
            long pending = inflight.incrementAndGet();
            while (pending > MAX_INFLIGHT) {
              LOG.info("Too many operations in flight, waiting");
              Thread.sleep(INFLIGHT_PAUSE_MS);
              pending = inflight.get();
            }
            final long putStartTime = System.currentTimeMillis();
            final CompletableFuture<Void> putFuture = table.put(put);
            addListener(putFuture, (r, e) -> {
              inflight.decrementAndGet();
              if (e == null) {
                output.getCounter(Counts.RPC_TIME_MS)
                  .increment(System.currentTimeMillis() - putStartTime);
                output.getCounter(Counts.RPC_BYTES_WRITTEN).increment(put.heapSize());
              }
            });

            // Write records out for later verification, one per HBase field except for the
            // content record, which will be verified by CRC64.
            output.write(
              new HBaseKeyWritable(rowKey, INFO_FAMILY_NAME, CONTENT_LENGTH_QUALIFIER, ts),
              new BytesWritable(Bytes.toBytes(content.length)));
            output.write(new HBaseKeyWritable(rowKey, INFO_FAMILY_NAME, CONTENT_TYPE_QUALIFIER, ts),
              new BytesWritable(Bytes.toBytes(contentType)));
            output.write(new HBaseKeyWritable(rowKey, INFO_FAMILY_NAME, CRC_QUALIFIER, ts),
              new BytesWritable(Bytes.toBytes(crc64)));
            output.write(new HBaseKeyWritable(rowKey, INFO_FAMILY_NAME, TARGET_URI_QUALIFIER, ts),
              new BytesWritable(Bytes.toBytes(targetURI)));
            output.write(new HBaseKeyWritable(rowKey, INFO_FAMILY_NAME, DATE_QUALIFIER, ts),
              new BytesWritable(Bytes.toBytes(warcHeader.getDateString())));
            if (ipAddr != null) {
              output.write(new HBaseKeyWritable(rowKey, INFO_FAMILY_NAME, IP_ADDRESS_QUALIFIER, ts),
                new BytesWritable(Bytes.toBytes(ipAddr)));
            }

            if (doIncrements) {
              // The URLs cf is not tracked for correctness. For now it is used only to exercise
              // Increments, to drive some read load during ingest. They can be verified with a
              // reducer to sum increments per row and then compare the final count to the table
              // data. This is left as a future exercise.
              final byte[] refQual = Bytes.add(REF_QUALIFIER, SEP, rowKey);
              for (String refUri : extractUrls(content)) {
                try {
                  byte[] urlRowKey = rowKeyFromTargetURI(refUri);
                  LOG.debug("  -> {}", refUri);
                  final Increment increment = new Increment(urlRowKey);
                  increment.setTimestamp(ts);
                  increment.addColumn(URL_FAMILY_NAME, refQual, 1);
                  pending = inflight.incrementAndGet();
                  while (pending > MAX_INFLIGHT) {
                    LOG.info("Too many operations in flight, waiting");
                    Thread.sleep(INFLIGHT_PAUSE_MS);
                    pending = inflight.get();
                  }
                  final long incrStartTime = System.currentTimeMillis();
                  final CompletableFuture<Result> incrFuture = table.increment(increment);
                  addListener(incrFuture, (r, e) -> {
                    inflight.decrementAndGet();
                    if (e == null) {
                      output.getCounter(Counts.RPC_TIME_MS)
                        .increment(System.currentTimeMillis() - incrStartTime);
                      output.getCounter(Counts.RPC_BYTES_WRITTEN).increment(increment.heapSize());
                    }
                  });
                } catch (IllegalArgumentException | URISyntaxException e) {
                  LOG.debug("Could not make a row key for URI " + refUri + ", ignoring", e);
                }
              }
            }
          }
        }
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

    int run(final Path inputDir) throws IOException, ClassNotFoundException, InterruptedException {
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
      for (Counts c : Counts.values()) {
        long value = counters.findCounter(c).getValue();
        if (value != 0) {
          LOG.info(c + ": " + value);
        }
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
      if (args.length < 2) {
        System.err.println(USAGE);
        return 1;
      }
      Path loaderOutput = new Path(args[0]);
      return run(loaderOutput);
    }

    public static void main(String[] args) throws Exception {
      ExitHandler.getInstance()
        .exit(ToolRunner.run(HBaseConfiguration.create(), new Verify(), args));
    }

    public static class VerifyMapper
      extends Mapper<HBaseKeyWritable, BytesWritable, NullWritable, NullWritable> {

      protected Connection conn;
      protected Table table;

      @Override
      protected void setup(final Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        conn = ConnectionFactory.createConnection(conf);
        table = conn.getTable(getTablename(conf));
      }

      @Override
      protected void cleanup(final Context context) throws IOException, InterruptedException {
        // Close the table
        try {
          table.close();
        } catch (Exception e) {
          LOG.warn("Exception closing table", e);
        }
        // Close the connection
        try {
          conn.close();
        } catch (Exception e) {
          LOG.warn("Exception closing Connection", e);
        }
      }

      @Override
      protected void map(final HBaseKeyWritable key, final BytesWritable value,
        final Context output) throws IOException, InterruptedException {
        final byte[] row = Bytes.copy(key.getRowArray(), key.getRowOffset(), key.getRowLength());
        final byte[] family =
          Bytes.copy(key.getFamilyArray(), key.getFamilyOffset(), key.getFamilyLength());
        final byte[] qualifier =
          Bytes.copy(key.getQualifierArray(), key.getQualifierOffset(), key.getQualifierLength());
        final long ts = key.getTimestamp();

        if (Bytes.equals(INFO_FAMILY_NAME, family) && Bytes.equals(CRC_QUALIFIER, qualifier)) {
          final long expectedCRC64 = Bytes.toLong(value.getBytes(), 0, value.getLength());
          final Get get = new Get(row).setTimestamp(ts).addFamily(CONTENT_FAMILY_NAME)
            .addFamily(INFO_FAMILY_NAME);
          final long startTime = System.currentTimeMillis();
          Result r;
          try {
            r = table.get(get);
            output.getCounter(Counts.RPC_TIME_MS).increment(System.currentTimeMillis() - startTime);
          } catch (Exception e) {
            LOG.error("Row " + Bytes.toStringBinary(row) + ": exception", e);
            output.getCounter(Counts.UNREFERENCED).increment(1);
            return;
          }
          final byte[] crcBytes = r.getValue(INFO_FAMILY_NAME, CRC_QUALIFIER);
          if (crcBytes == null) {
            LOG.error("Row " + Bytes.toStringBinary(row) + ": missing i:c");
            output.getCounter(Counts.UNREFERENCED).increment(1);
            return;
          }
          if (Bytes.toLong(crcBytes) != expectedCRC64) {
            LOG.error("Row " + Bytes.toStringBinary(row) + ": i:c mismatch");
            output.getCounter(Counts.CORRUPT).increment(1);
            return;
          }
          // If we fell through to here all verification checks have succeeded for the info
          // record.
          output.getCounter(Counts.REFERENCED).increment(1);
          final byte[] content = r.getValue(CONTENT_FAMILY_NAME, CONTENT_QUALIFIER);
          if (content == null) {
            LOG.error("Row " + Bytes.toStringBinary(row) + ": missing content");
            output.getCounter(Counts.UNREFERENCED).increment(1);
            return;
          } else {
            final CRC64 crc = new CRC64();
            crc.update(content);
            if (crc.getValue() != expectedCRC64) {
              LOG.error("Row " + Bytes.toStringBinary(row) + ": corrupt content");
              output.getCounter(Counts.CORRUPT).increment(1);
              return;
            }
          }
          // If we fell through to here all verification checks have succeeded for the content
          // record.
          output.getCounter(Counts.REFERENCED).increment(1);
        } else {
          final long startTime = System.currentTimeMillis();
          final Get get = new Get(row).setTimestamp(ts).addColumn(family, qualifier);
          Result r;
          try {
            r = table.get(get);
            output.getCounter(Counts.RPC_TIME_MS).increment(System.currentTimeMillis() - startTime);
          } catch (Exception e) {
            LOG.error("Row " + Bytes.toStringBinary(row) + ": exception", e);
            output.getCounter(Counts.UNREFERENCED).increment(1);
            return;
          }
          final byte[] bytes = r.getValue(family, qualifier);
          if (bytes == null) {
            LOG.error("Row " + Bytes.toStringBinary(row) + ": missing "
              + Bytes.toStringBinary(family) + ":" + Bytes.toStringBinary(qualifier));
            output.getCounter(Counts.UNREFERENCED).increment(1);
            return;
          }
          if (!Bytes.equals(bytes, 0, bytes.length, value.getBytes(), 0, value.getLength())) {
            LOG.error("Row " + Bytes.toStringBinary(row) + ": " + Bytes.toStringBinary(family) + ":"
              + Bytes.toStringBinary(qualifier) + " mismatch");
            output.getCounter(Counts.CORRUPT).increment(1);
            return;
          }
          // If we fell through to here all verification checks have succeeded for the info
          // record.
          output.getCounter(Counts.REFERENCED).increment(1);
        }
      }
    }
  }

  private static final AtomicLong counter = new AtomicLong();
  private static final int shift = 8;

  private static long getSequence() {
    long t = EnvironmentEdgeManager.currentTime();
    t <<= shift;
    t |= (counter.getAndIncrement() % (1 << shift));
    return t;
  }

  private static byte[] rowKeyFromTargetURI(final String targetUri)
    throws IOException, URISyntaxException, IllegalArgumentException {
    final URI uri = new URI(targetUri);
    // Ignore the scheme
    // Reverse the components of the hostname
    String reversedHost;
    if (uri.getHost() != null) {
      final String[] hostComponents =
        Splitter.on('.').splitToStream(uri.getHost()).toArray(String[]::new);
      final StringBuilder sb = new StringBuilder();
      for (int i = hostComponents.length - 1; i >= 0; i--) {
        sb.append(hostComponents[i]);
        if (i != 0) {
          sb.append('.');
        }
      }
      reversedHost = sb.toString();
    } else {
      throw new IllegalArgumentException("URI is missing host component");
    }
    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    os.write(reversedHost.getBytes(StandardCharsets.UTF_8));
    if (uri.getPort() >= 0) {
      os.write(String.format(":%d", uri.getPort()).getBytes(StandardCharsets.UTF_8));
    }
    os.write((byte) '|');
    if (uri.getPath() != null) {
      os.write(uri.getPath().getBytes(StandardCharsets.UTF_8));
    }
    if (uri.getQuery() != null) {
      os.write(String.format("?%s", uri.getQuery()).getBytes(StandardCharsets.UTF_8));
    }
    if (uri.getFragment() != null) {
      os.write(String.format("#%s", uri.getFragment()).getBytes(StandardCharsets.UTF_8));
    }
    if (os.size() > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException(
        "Key would be too large (length=" + os.size() + ", limit=" + HConstants.MAX_ROW_LENGTH);
    }
    return os.toByteArray();
  }

  static final Pattern URL_PATTERN = Pattern.compile(
    "\\b((https?|ftp|file)://|(www|ftp)\\.)" + "[\\-A-Z0-9+&@#/%?=~_|$!:,\\.;]*[A-Z0-9+&@#/%=~_|$]",
    Pattern.CASE_INSENSITIVE);

  private static Collection<String> extractUrls(byte[] content) {
    final Set<String> list = new HashSet<>(); // uniques
    final Matcher m = URL_PATTERN.matcher(new String(content, StandardCharsets.UTF_8));
    while (m.find()) {
      list.add(m.group());
    }
    return list;
  }

}
