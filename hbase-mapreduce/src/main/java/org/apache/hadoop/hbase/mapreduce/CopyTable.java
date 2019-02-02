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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.Import.CellImporter;
import org.apache.hadoop.hbase.mapreduce.Import.Importer;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool used to copy a table to another one which can be on a different setup.
 * It is also configurable with a start and time as well as a specification
 * of the region server implementation if different from the local cluster.
 */
@InterfaceAudience.Public
public class CopyTable extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(CopyTable.class);

  final static String NAME = "copytable";
  long startTime = 0;
  long endTime = HConstants.LATEST_TIMESTAMP;
  int batch = Integer.MAX_VALUE;
  int cacheRow = -1;
  int versions = -1;
  String tableName = null;
  String startRow = null;
  String stopRow = null;
  String dstTableName = null;
  String peerAddress = null;
  String families = null;
  boolean allCells = false;
  static boolean shuffle = false;

  boolean bulkload = false;
  Path bulkloadDir = null;

  boolean readingSnapshot = false;
  String snapshot = null;

  private final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";

  private Path generateUniqTempDir(boolean withDirCreated) throws IOException {
    FileSystem fs = FSUtils.getCurrentFileSystem(getConf());
    Path dir = new Path(fs.getWorkingDirectory(), NAME);
    if (!fs.exists(dir)) {
      fs.mkdirs(dir);
    }
    Path newDir = new Path(dir, UUID.randomUUID().toString());
    if (withDirCreated) {
      fs.mkdirs(newDir);
    }
    return newDir;
  }

  private void initCopyTableMapperReducerJob(Job job, Scan scan) throws IOException {
    Class<? extends TableMapper> mapper = bulkload ? CellImporter.class : Importer.class;
    if (readingSnapshot) {
      TableMapReduceUtil.initTableSnapshotMapperJob(snapshot, scan, mapper, null, null, job, true,
        generateUniqTempDir(true));
    } else {
      TableMapReduceUtil.initTableMapperJob(tableName, scan, mapper, null, null, job);
    }
  }

  /**
   * Sets up the actual job.
   *
   * @param args  The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public Job createSubmittableJob(String[] args) throws IOException {
    if (!doCommandLine(args)) {
      return null;
    }

    String jobName = NAME + "_" + (tableName == null ? snapshot : tableName);
    Job job = Job.getInstance(getConf(), getConf().get(JOB_NAME_CONF_KEY, jobName));
    job.setJarByClass(CopyTable.class);
    Scan scan = new Scan();

    scan.setBatch(batch);
    scan.setCacheBlocks(false);

    if (cacheRow > 0) {
      scan.setCaching(cacheRow);
    } else {
      scan.setCaching(getConf().getInt(HConstants.HBASE_CLIENT_SCANNER_CACHING, 100));
    }

    scan.setTimeRange(startTime, endTime);

    if (allCells) {
      scan.setRaw(true);
    }
    if (shuffle) {
      job.getConfiguration().set(TableInputFormat.SHUFFLE_MAPS, "true");
    }
    if (versions >= 0) {
      scan.readVersions(versions);
    }

    if (startRow != null) {
      scan.withStartRow(Bytes.toBytesBinary(startRow));
    }

    if (stopRow != null) {
      scan.withStopRow(Bytes.toBytesBinary(stopRow));
    }

    if(families != null) {
      String[] fams = families.split(",");
      Map<String,String> cfRenameMap = new HashMap<>();
      for(String fam : fams) {
        String sourceCf;
        if(fam.contains(":")) {
            // fam looks like "sourceCfName:destCfName"
            String[] srcAndDest = fam.split(":", 2);
            sourceCf = srcAndDest[0];
            String destCf = srcAndDest[1];
            cfRenameMap.put(sourceCf, destCf);
        } else {
            // fam is just "sourceCf"
            sourceCf = fam;
        }
        scan.addFamily(Bytes.toBytes(sourceCf));
      }
      Import.configureCfRenaming(job.getConfiguration(), cfRenameMap);
    }
    job.setNumReduceTasks(0);

    if (bulkload) {
      initCopyTableMapperReducerJob(job, scan);

      // We need to split the inputs by destination tables so that output of Map can be bulk-loaded.
      TableInputFormat.configureSplitTable(job, TableName.valueOf(dstTableName));

      bulkloadDir = generateUniqTempDir(false);
      LOG.info("HFiles will be stored at " + this.bulkloadDir);
      HFileOutputFormat2.setOutputPath(job, bulkloadDir);
      try (Connection conn = ConnectionFactory.createConnection(getConf());
          Admin admin = conn.getAdmin()) {
        HFileOutputFormat2.configureIncrementalLoadMap(job,
          admin.getDescriptor((TableName.valueOf(dstTableName))));
      }
    } else {
      initCopyTableMapperReducerJob(job, scan);
      TableMapReduceUtil.initTableReducerJob(dstTableName, null, job, null, peerAddress, null,
        null);
    }

    return job;
  }

  /*
   * @param errorMsg Error message.  Can be null.
   */
  private static void printUsage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: CopyTable [general options] [--starttime=X] [--endtime=Y] " +
        "[--new.name=NEW] [--peer.adr=ADR] <tablename | snapshotName>");
    System.err.println();
    System.err.println("Options:");
    System.err.println(" rs.class     hbase.regionserver.class of the peer cluster");
    System.err.println("              specify if different from current cluster");
    System.err.println(" rs.impl      hbase.regionserver.impl of the peer cluster");
    System.err.println(" startrow     the start row");
    System.err.println(" stoprow      the stop row");
    System.err.println(" starttime    beginning of the time range (unixtime in millis)");
    System.err.println("              without endtime means from starttime to forever");
    System.err.println(" endtime      end of the time range.  Ignored if no starttime specified.");
    System.err.println(" versions     number of cell versions to copy");
    System.err.println(" new.name     new table's name");
    System.err.println(" peer.adr     Address of the peer cluster given in the format");
    System.err.println("              hbase.zookeeper.quorum:hbase.zookeeper.client"
        + ".port:zookeeper.znode.parent");
    System.err.println(" families     comma-separated list of families to copy");
    System.err.println("              To copy from cf1 to cf2, give sourceCfName:destCfName. ");
    System.err.println("              To keep the same name, just give \"cfName\"");
    System.err.println(" all.cells    also copy delete markers and deleted cells");
    System.err.println(" bulkload     Write input into HFiles and bulk load to the destination "
        + "table");
    System.err.println(" snapshot     Copy the data from snapshot to destination table.");
    System.err.println();
    System.err.println("Args:");
    System.err.println(" tablename    Name of the table to copy");
    System.err.println();
    System.err.println("Examples:");
    System.err.println(" To copy 'TestTable' to a cluster that uses replication for a 1 hour window:");
    System.err.println(" $ hbase " +
        "org.apache.hadoop.hbase.mapreduce.CopyTable --starttime=1265875194289 --endtime=1265878794289 " +
        "--peer.adr=server1,server2,server3:2181:/hbase --families=myOldCf:myNewCf,cf2,cf3 TestTable ");
    System.err.println(" To copy data from 'sourceTableSnapshot' to 'destTable': ");
    System.err.println(" $ hbase org.apache.hadoop.hbase.mapreduce.CopyTable "
        + "--snapshot --new.name=destTable sourceTableSnapshot");
    System.err.println(" To copy data from 'sourceTableSnapshot' and bulk load to 'destTable': ");
    System.err.println(" $ hbase org.apache.hadoop.hbase.mapreduce.CopyTable "
        + "--new.name=destTable --snapshot --bulkload sourceTableSnapshot");
    System.err.println("For performance consider the following general option:\n"
        + "  It is recommended that you set the following to >=100. A higher value uses more memory but\n"
        + "  decreases the round trip time to the server and may increase performance.\n"
        + "    -Dhbase.client.scanner.caching=100\n"
        + "  The following should always be set to false, to prevent writing data twice, which may produce \n"
        + "  inaccurate results.\n"
        + "    -Dmapreduce.map.speculative=false");
  }

  private boolean doCommandLine(final String[] args) {
    if (args.length < 1) {
      printUsage(null);
      return false;
    }
    try {
      for (int i = 0; i < args.length; i++) {
        String cmd = args[i];
        if (cmd.equals("-h") || cmd.startsWith("--h")) {
          printUsage(null);
          return false;
        }

        final String startRowArgKey = "--startrow=";
        if (cmd.startsWith(startRowArgKey)) {
          startRow = cmd.substring(startRowArgKey.length());
          continue;
        }

        final String stopRowArgKey = "--stoprow=";
        if (cmd.startsWith(stopRowArgKey)) {
          stopRow = cmd.substring(stopRowArgKey.length());
          continue;
        }

        final String startTimeArgKey = "--starttime=";
        if (cmd.startsWith(startTimeArgKey)) {
          startTime = Long.parseLong(cmd.substring(startTimeArgKey.length()));
          continue;
        }

        final String endTimeArgKey = "--endtime=";
        if (cmd.startsWith(endTimeArgKey)) {
          endTime = Long.parseLong(cmd.substring(endTimeArgKey.length()));
          continue;
        }

        final String batchArgKey = "--batch=";
        if (cmd.startsWith(batchArgKey)) {
          batch = Integer.parseInt(cmd.substring(batchArgKey.length()));
          continue;
        }

        final String cacheRowArgKey = "--cacheRow=";
        if (cmd.startsWith(cacheRowArgKey)) {
          cacheRow = Integer.parseInt(cmd.substring(cacheRowArgKey.length()));
          continue;
        }

        final String versionsArgKey = "--versions=";
        if (cmd.startsWith(versionsArgKey)) {
          versions = Integer.parseInt(cmd.substring(versionsArgKey.length()));
          continue;
        }

        final String newNameArgKey = "--new.name=";
        if (cmd.startsWith(newNameArgKey)) {
          dstTableName = cmd.substring(newNameArgKey.length());
          continue;
        }

        final String peerAdrArgKey = "--peer.adr=";
        if (cmd.startsWith(peerAdrArgKey)) {
          peerAddress = cmd.substring(peerAdrArgKey.length());
          continue;
        }

        final String familiesArgKey = "--families=";
        if (cmd.startsWith(familiesArgKey)) {
          families = cmd.substring(familiesArgKey.length());
          continue;
        }

        if (cmd.startsWith("--all.cells")) {
          allCells = true;
          continue;
        }

        if (cmd.startsWith("--bulkload")) {
          bulkload = true;
          continue;
        }

        if (cmd.startsWith("--shuffle")) {
          shuffle = true;
          continue;
        }

        if(cmd.startsWith("--snapshot")){
          readingSnapshot = true;
          continue;
        }

        if (i == args.length - 1) {
          if (readingSnapshot) {
            snapshot = cmd;
          } else {
            tableName = cmd;
          }
        } else {
          printUsage("Invalid argument '" + cmd + "'");
          return false;
        }
      }
      if (dstTableName == null && peerAddress == null) {
        printUsage("At least a new table name or a peer address must be specified");
        return false;
      }
      if ((endTime != 0) && (startTime > endTime)) {
        printUsage("Invalid time range filter: starttime=" + startTime + " >  endtime=" + endTime);
        return false;
      }

      if (bulkload && peerAddress != null) {
        printUsage("Remote bulkload is not supported!");
        return false;
      }

      if (readingSnapshot && peerAddress != null) {
        printUsage("Loading data from snapshot to remote peer cluster is not supported.");
        return false;
      }

      if (readingSnapshot && dstTableName == null) {
        printUsage("The --new.name=<table> for destination table should be "
            + "provided when copying data from snapshot .");
        return false;
      }

      if (readingSnapshot && snapshot == null) {
        printUsage("Snapshot shouldn't be null when --snapshot is enabled.");
        return false;
      }

      // set dstTableName if necessary
      if (dstTableName == null) {
        dstTableName = tableName;
      }
    } catch (Exception e) {
      LOG.error("Failed to parse commandLine arguments", e);
      printUsage("Can't start because " + e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(HBaseConfiguration.create(), new CopyTable(), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = createSubmittableJob(args);
    if (job == null) return 1;
    if (!job.waitForCompletion(true)) {
      LOG.info("Map-reduce job failed!");
      if (bulkload) {
        LOG.info("Files are not bulkloaded!");
      }
      return 1;
    }
    int code = 0;
    if (bulkload) {
      LOG.info("Trying to bulk load data to destination table: " + dstTableName);
      LOG.info("command: ./bin/hbase {} {} {}", BulkLoadHFilesTool.NAME,
        this.bulkloadDir.toString(), this.dstTableName);
      if (!BulkLoadHFiles.create(getConf()).bulkLoad(TableName.valueOf(dstTableName), bulkloadDir)
        .isEmpty()) {
        // bulkloadDir is deleted only BulkLoadHFiles was successful so that one can rerun
        // BulkLoadHFiles.
        FileSystem fs = FSUtils.getCurrentFileSystem(getConf());
        if (!fs.delete(this.bulkloadDir, true)) {
          LOG.error("Deleting folder " + bulkloadDir + " failed!");
          code = 1;
        }
      }
    }
    return code;
  }
}
