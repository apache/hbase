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
package org.apache.hadoop.hbase.mapreduce.replication;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableSnapshotScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This map-only job compares the data from a local table with a remote one.
 * Every cell is compared and must have exactly the same keys (even timestamp)
 * as well as same value. It is possible to restrict the job by time range and
 * families. The peer id that's provided must match the one given when the
 * replication stream was setup.
 * <p>
 * Two counters are provided, Verifier.Counters.GOODROWS and BADROWS. The reason
 * for a why a row is different is shown in the map's log.
 */
@InterfaceAudience.Private
public class VerifyReplication extends Configured implements Tool {

  private static final Logger LOG =
      LoggerFactory.getLogger(VerifyReplication.class);

  public final static String NAME = "verifyrep";
  private final static String PEER_CONFIG_PREFIX = NAME + ".peer.";
  long startTime = 0;
  long endTime = Long.MAX_VALUE;
  int batch = -1;
  int versions = -1;
  String tableName = null;
  String families = null;
  String delimiter = "";
  String peerId = null;
  String peerQuorumAddress = null;
  String rowPrefixes = null;
  int sleepMsBeforeReCompare = 0;
  boolean verbose = false;
  boolean includeDeletedCells = false;
  //Source table snapshot name
  String sourceSnapshotName = null;
  //Temp location in source cluster to restore source snapshot
  String sourceSnapshotTmpDir = null;
  //Peer table snapshot name
  String peerSnapshotName = null;
  //Temp location in peer cluster to restore peer snapshot
  String peerSnapshotTmpDir = null;
  //Peer cluster Hadoop FS address
  String peerFSAddress = null;
  //Peer cluster HBase root dir location
  String peerHBaseRootAddress = null;
  //Peer Table Name
  String peerTableName = null;


  private final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";

  /**
   * Map-only comparator for 2 tables
   */
  public static class Verifier
      extends TableMapper<ImmutableBytesWritable, Put> {

    public enum Counters {
      GOODROWS, BADROWS, ONLY_IN_SOURCE_TABLE_ROWS, ONLY_IN_PEER_TABLE_ROWS, CONTENT_DIFFERENT_ROWS
    }

    private Connection sourceConnection;
    private Table sourceTable;
    private Connection replicatedConnection;
    private Table replicatedTable;
    private ResultScanner replicatedScanner;
    private Result currentCompareRowInPeerTable;
    private int sleepMsBeforeReCompare;
    private String delimiter = "";
    private boolean verbose = false;
    private int batch = -1;

    /**
     * Map method that compares every scanned row with the equivalent from
     * a distant cluster.
     * @param row  The current table row key.
     * @param value  The columns.
     * @param context  The current context.
     * @throws IOException When something is broken with the data.
     */
    @Override
    public void map(ImmutableBytesWritable row, final Result value,
                    Context context)
        throws IOException {
      if (replicatedScanner == null) {
        Configuration conf = context.getConfiguration();
        sleepMsBeforeReCompare = conf.getInt(NAME +".sleepMsBeforeReCompare", 0);
        delimiter = conf.get(NAME + ".delimiter", "");
        verbose = conf.getBoolean(NAME +".verbose", false);
        batch = conf.getInt(NAME + ".batch", -1);
        final Scan scan = new Scan();
        if (batch > 0) {
          scan.setBatch(batch);
        }
        scan.setCacheBlocks(false);
        scan.setCaching(conf.getInt(TableInputFormat.SCAN_CACHEDROWS, 1));
        long startTime = conf.getLong(NAME + ".startTime", 0);
        long endTime = conf.getLong(NAME + ".endTime", Long.MAX_VALUE);
        String families = conf.get(NAME + ".families", null);
        if(families != null) {
          String[] fams = families.split(",");
          for(String fam : fams) {
            scan.addFamily(Bytes.toBytes(fam));
          }
        }
        boolean includeDeletedCells = conf.getBoolean(NAME + ".includeDeletedCells", false);
        scan.setRaw(includeDeletedCells);
        String rowPrefixes = conf.get(NAME + ".rowPrefixes", null);
        setRowPrefixFilter(scan, rowPrefixes);
        scan.setTimeRange(startTime, endTime);
        int versions = conf.getInt(NAME+".versions", -1);
        LOG.info("Setting number of version inside map as: " + versions);
        if (versions >= 0) {
          scan.setMaxVersions(versions);
        }
        TableName tableName = TableName.valueOf(conf.get(NAME + ".tableName"));
        sourceConnection = ConnectionFactory.createConnection(conf);
        sourceTable = sourceConnection.getTable(tableName);

        final InputSplit tableSplit = context.getInputSplit();

        String zkClusterKey = conf.get(NAME + ".peerQuorumAddress");
        Configuration peerConf = HBaseConfiguration.createClusterConf(conf,
            zkClusterKey, PEER_CONFIG_PREFIX);

        String peerName = peerConf.get(NAME + ".peerTableName", tableName.getNameAsString());
        TableName peerTableName = TableName.valueOf(peerName);
        replicatedConnection = ConnectionFactory.createConnection(peerConf);
        replicatedTable = replicatedConnection.getTable(peerTableName);
        scan.setStartRow(value.getRow());

        byte[] endRow = null;
        if (tableSplit instanceof TableSnapshotInputFormat.TableSnapshotRegionSplit) {
          endRow = ((TableSnapshotInputFormat.TableSnapshotRegionSplit) tableSplit).getRegionInfo()
              .getEndKey();
        } else {
          endRow = ((TableSplit) tableSplit).getEndRow();
        }

        scan.setStopRow(endRow);

        String peerSnapshotName = conf.get(NAME + ".peerSnapshotName", null);
        if (peerSnapshotName != null) {
          String peerSnapshotTmpDir = conf.get(NAME + ".peerSnapshotTmpDir", null);
          String peerFSAddress = conf.get(NAME + ".peerFSAddress", null);
          String peerHBaseRootAddress = conf.get(NAME + ".peerHBaseRootAddress", null);
          FileSystem.setDefaultUri(peerConf, peerFSAddress);
          CommonFSUtils.setRootDir(peerConf, new Path(peerHBaseRootAddress));
          LOG.info("Using peer snapshot:" + peerSnapshotName + " with temp dir:" +
            peerSnapshotTmpDir + " peer root uri:" + CommonFSUtils.getRootDir(peerConf) +
            " peerFSAddress:" + peerFSAddress);

          replicatedScanner = new TableSnapshotScanner(peerConf, CommonFSUtils.getRootDir(peerConf),
            new Path(peerFSAddress, peerSnapshotTmpDir), peerSnapshotName, scan, true);
        } else {
          replicatedScanner = replicatedTable.getScanner(scan);
        }
        currentCompareRowInPeerTable = replicatedScanner.next();
      }
      while (true) {
        if (currentCompareRowInPeerTable == null) {
          // reach the region end of peer table, row only in source table
          logFailRowAndIncreaseCounter(context, Counters.ONLY_IN_SOURCE_TABLE_ROWS, value);
          break;
        }
        int rowCmpRet = Bytes.compareTo(value.getRow(), currentCompareRowInPeerTable.getRow());
        if (rowCmpRet == 0) {
          // rowkey is same, need to compare the content of the row
          try {
            Result.compareResults(value, currentCompareRowInPeerTable, false);
            context.getCounter(Counters.GOODROWS).increment(1);
            if (verbose) {
              LOG.info("Good row key: " + delimiter
                  + Bytes.toStringBinary(value.getRow()) + delimiter);
            }
          } catch (Exception e) {
            logFailRowAndIncreaseCounter(context, Counters.CONTENT_DIFFERENT_ROWS, value);
          }
          currentCompareRowInPeerTable = replicatedScanner.next();
          break;
        } else if (rowCmpRet < 0) {
          // row only exists in source table
          logFailRowAndIncreaseCounter(context, Counters.ONLY_IN_SOURCE_TABLE_ROWS, value);
          break;
        } else {
          // row only exists in peer table
          logFailRowAndIncreaseCounter(context, Counters.ONLY_IN_PEER_TABLE_ROWS,
            currentCompareRowInPeerTable);
          currentCompareRowInPeerTable = replicatedScanner.next();
        }
      }
    }

    private void logFailRowAndIncreaseCounter(Context context, Counters counter, Result row) {
      if (sleepMsBeforeReCompare > 0) {
        Threads.sleep(sleepMsBeforeReCompare);
        try {
          Result sourceResult = sourceTable.get(new Get(row.getRow()));
          Result replicatedResult = replicatedTable.get(new Get(row.getRow()));
          Result.compareResults(sourceResult, replicatedResult, false);
          if (!sourceResult.isEmpty()) {
            context.getCounter(Counters.GOODROWS).increment(1);
            if (verbose) {
              LOG.info("Good row key (with recompare): " + delimiter +
                Bytes.toStringBinary(row.getRow())
              + delimiter);
            }
          }
          return;
        } catch (Exception e) {
          LOG.error("recompare fail after sleep, rowkey=" + delimiter +
              Bytes.toStringBinary(row.getRow()) + delimiter);
        }
      }
      context.getCounter(counter).increment(1);
      context.getCounter(Counters.BADROWS).increment(1);
      LOG.error(counter.toString() + ", rowkey=" + delimiter + Bytes.toStringBinary(row.getRow()) +
          delimiter);
    }

    @Override
    protected void cleanup(Context context) {
      if (replicatedScanner != null) {
        try {
          while (currentCompareRowInPeerTable != null) {
            logFailRowAndIncreaseCounter(context, Counters.ONLY_IN_PEER_TABLE_ROWS,
              currentCompareRowInPeerTable);
            currentCompareRowInPeerTable = replicatedScanner.next();
          }
        } catch (Exception e) {
          LOG.error("fail to scan peer table in cleanup", e);
        } finally {
          replicatedScanner.close();
          replicatedScanner = null;
        }
      }

      if (sourceTable != null) {
        try {
          sourceTable.close();
        } catch (IOException e) {
          LOG.error("fail to close source table in cleanup", e);
        }
      }
      if(sourceConnection != null){
        try {
          sourceConnection.close();
        } catch (Exception e) {
          LOG.error("fail to close source connection in cleanup", e);
        }
      }

      if(replicatedTable != null){
        try{
          replicatedTable.close();
        } catch (Exception e) {
          LOG.error("fail to close replicated table in cleanup", e);
        }
      }
      if(replicatedConnection != null){
        try {
          replicatedConnection.close();
        } catch (Exception e) {
          LOG.error("fail to close replicated connection in cleanup", e);
        }
      }
    }
  }

  private static Pair<ReplicationPeerConfig, Configuration> getPeerQuorumConfig(
      final Configuration conf, String peerId) throws IOException {
    ZKWatcher localZKW = null;
    try {
      localZKW = new ZKWatcher(conf, "VerifyReplication", new Abortable() {
        @Override
        public void abort(String why, Throwable e) {
        }

        @Override
        public boolean isAborted() {
          return false;
        }
      });
      ReplicationPeerStorage storage =
        ReplicationStorageFactory.getReplicationPeerStorage(localZKW, conf);
      ReplicationPeerConfig peerConfig = storage.getPeerConfig(peerId);
      return Pair.newPair(peerConfig,
        ReplicationUtils.getPeerClusterConfiguration(peerConfig, conf));
    } catch (ReplicationException e) {
      throw new IOException("An error occurred while trying to connect to the remote peer cluster",
          e);
    } finally {
      if (localZKW != null) {
        localZKW.close();
      }
    }
  }

  private void restoreSnapshotForPeerCluster(Configuration conf, String peerQuorumAddress)
    throws IOException {
    Configuration peerConf =
      HBaseConfiguration.createClusterConf(conf, peerQuorumAddress, PEER_CONFIG_PREFIX);
    FileSystem.setDefaultUri(peerConf, peerFSAddress);
    CommonFSUtils.setRootDir(peerConf, new Path(peerFSAddress, peerHBaseRootAddress));
    FileSystem fs = FileSystem.get(peerConf);
    RestoreSnapshotHelper.copySnapshotForScanner(peerConf, fs, CommonFSUtils.getRootDir(peerConf),
      new Path(peerFSAddress, peerSnapshotTmpDir), peerSnapshotName);
  }

  /**
   * Sets up the actual job.
   *
   * @param conf  The current configuration.
   * @param args  The command line parameters.
   * @return The newly created job.
   * @throws java.io.IOException When setting up the job fails.
   */
  public Job createSubmittableJob(Configuration conf, String[] args)
  throws IOException {
    if (!doCommandLine(args)) {
      return null;
    }
    conf.set(NAME+".tableName", tableName);
    conf.setLong(NAME+".startTime", startTime);
    conf.setLong(NAME+".endTime", endTime);
    conf.setInt(NAME +".sleepMsBeforeReCompare", sleepMsBeforeReCompare);
    conf.set(NAME + ".delimiter", delimiter);
    conf.setInt(NAME + ".batch", batch);
    conf.setBoolean(NAME +".verbose", verbose);
    conf.setBoolean(NAME +".includeDeletedCells", includeDeletedCells);
    if (families != null) {
      conf.set(NAME+".families", families);
    }
    if (rowPrefixes != null){
      conf.set(NAME+".rowPrefixes", rowPrefixes);
    }

    String peerQuorumAddress;
    Pair<ReplicationPeerConfig, Configuration> peerConfigPair = null;
    if (peerId != null) {
      peerConfigPair = getPeerQuorumConfig(conf, peerId);
      ReplicationPeerConfig peerConfig = peerConfigPair.getFirst();
      peerQuorumAddress = peerConfig.getClusterKey();
      LOG.info("Peer Quorum Address: " + peerQuorumAddress + ", Peer Configuration: " +
        peerConfig.getConfiguration());
      conf.set(NAME + ".peerQuorumAddress", peerQuorumAddress);
      HBaseConfiguration.setWithPrefix(conf, PEER_CONFIG_PREFIX,
        peerConfig.getConfiguration().entrySet());
    } else {
      assert this.peerQuorumAddress != null;
      peerQuorumAddress = this.peerQuorumAddress;
      LOG.info("Peer Quorum Address: " + peerQuorumAddress);
      conf.set(NAME + ".peerQuorumAddress", peerQuorumAddress);
    }

    if (peerTableName != null) {
      LOG.info("Peer Table Name: " + peerTableName);
      conf.set(NAME + ".peerTableName", peerTableName);
    }

    conf.setInt(NAME + ".versions", versions);
    LOG.info("Number of version: " + versions);

    //Set Snapshot specific parameters
    if (peerSnapshotName != null) {
      conf.set(NAME + ".peerSnapshotName", peerSnapshotName);

      // for verifyRep by snapshot, choose a unique sub-directory under peerSnapshotTmpDir to
      // restore snapshot.
      Path restoreDir = new Path(peerSnapshotTmpDir, UUID.randomUUID().toString());
      peerSnapshotTmpDir = restoreDir.toString();
      conf.set(NAME + ".peerSnapshotTmpDir", peerSnapshotTmpDir);

      conf.set(NAME + ".peerFSAddress", peerFSAddress);
      conf.set(NAME + ".peerHBaseRootAddress", peerHBaseRootAddress);

      // This is to create HDFS delegation token for peer cluster in case of secured
      conf.setStrings(MRJobConfig.JOB_NAMENODES, peerFSAddress, conf.get(HConstants.HBASE_DIR));
    }

    Job job = Job.getInstance(conf, conf.get(JOB_NAME_CONF_KEY, NAME + "_" + tableName));
    job.setJarByClass(VerifyReplication.class);

    Scan scan = new Scan();
    scan.setTimeRange(startTime, endTime);
    scan.setRaw(includeDeletedCells);
    scan.setCacheBlocks(false);
    if (batch > 0) {
      scan.setBatch(batch);
    }
    if (versions >= 0) {
      scan.setMaxVersions(versions);
      LOG.info("Number of versions set to " + versions);
    }
    if(families != null) {
      String[] fams = families.split(",");
      for(String fam : fams) {
        scan.addFamily(Bytes.toBytes(fam));
      }
    }

    setRowPrefixFilter(scan, rowPrefixes);

    if (sourceSnapshotName != null) {
      Path snapshotTempPath = new Path(sourceSnapshotTmpDir);
      LOG.info(
        "Using source snapshot-" + sourceSnapshotName + " with temp dir:" + sourceSnapshotTmpDir);
      TableMapReduceUtil.initTableSnapshotMapperJob(sourceSnapshotName, scan, Verifier.class, null,
        null, job, true, snapshotTempPath);
      restoreSnapshotForPeerCluster(conf, peerQuorumAddress);
    } else {
      TableMapReduceUtil.initTableMapperJob(tableName, scan, Verifier.class, null, null, job);
    }

    Configuration peerClusterConf;
    if (peerId != null) {
      assert peerConfigPair != null;
      peerClusterConf = peerConfigPair.getSecond();
    } else {
      peerClusterConf = HBaseConfiguration.createClusterConf(conf,
        peerQuorumAddress, PEER_CONFIG_PREFIX);
    }
    // Obtain the auth token from peer cluster
    TableMapReduceUtil.initCredentialsForCluster(job, peerClusterConf);

    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);
    return job;
  }

  private static void setRowPrefixFilter(Scan scan, String rowPrefixes) {
    if (rowPrefixes != null && !rowPrefixes.isEmpty()) {
      String[] rowPrefixArray = rowPrefixes.split(",");
      Arrays.sort(rowPrefixArray);
      FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
      for (String prefix : rowPrefixArray) {
        Filter filter = new PrefixFilter(Bytes.toBytes(prefix));
        filterList.addFilter(filter);
      }
      scan.setFilter(filterList);
      byte[] startPrefixRow = Bytes.toBytes(rowPrefixArray[0]);
      byte[] lastPrefixRow = Bytes.toBytes(rowPrefixArray[rowPrefixArray.length -1]);
      setStartAndStopRows(scan, startPrefixRow, lastPrefixRow);
    }
  }

  private static void setStartAndStopRows(Scan scan, byte[] startPrefixRow, byte[] lastPrefixRow) {
    scan.setStartRow(startPrefixRow);
    byte[] stopRow = Bytes.add(Bytes.head(lastPrefixRow, lastPrefixRow.length - 1),
        new byte[]{(byte) (lastPrefixRow[lastPrefixRow.length - 1] + 1)});
    scan.setStopRow(stopRow);
  }

  public boolean doCommandLine(final String[] args) {
    if (args.length < 2) {
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

        final String includeDeletedCellsArgKey = "--raw";
        if (cmd.equals(includeDeletedCellsArgKey)) {
          includeDeletedCells = true;
          continue;
        }

        final String versionsArgKey = "--versions=";
        if (cmd.startsWith(versionsArgKey)) {
          versions = Integer.parseInt(cmd.substring(versionsArgKey.length()));
          continue;
        }

        final String batchArgKey = "--batch=";
        if (cmd.startsWith(batchArgKey)) {
          batch = Integer.parseInt(cmd.substring(batchArgKey.length()));
          continue;
        }

        final String familiesArgKey = "--families=";
        if (cmd.startsWith(familiesArgKey)) {
          families = cmd.substring(familiesArgKey.length());
          continue;
        }

        final String rowPrefixesKey = "--row-prefixes=";
        if (cmd.startsWith(rowPrefixesKey)){
          rowPrefixes = cmd.substring(rowPrefixesKey.length());
          continue;
        }

        final String delimiterArgKey = "--delimiter=";
        if (cmd.startsWith(delimiterArgKey)) {
          delimiter = cmd.substring(delimiterArgKey.length());
          continue;
        }

        final String sleepToReCompareKey = "--recomparesleep=";
        if (cmd.startsWith(sleepToReCompareKey)) {
          sleepMsBeforeReCompare = Integer.parseInt(cmd.substring(sleepToReCompareKey.length()));
          continue;
        }
        final String verboseKey = "--verbose";
        if (cmd.startsWith(verboseKey)) {
          verbose = true;
          continue;
        }

        final String sourceSnapshotNameArgKey = "--sourceSnapshotName=";
        if (cmd.startsWith(sourceSnapshotNameArgKey)) {
          sourceSnapshotName = cmd.substring(sourceSnapshotNameArgKey.length());
          continue;
        }

        final String sourceSnapshotTmpDirArgKey = "--sourceSnapshotTmpDir=";
        if (cmd.startsWith(sourceSnapshotTmpDirArgKey)) {
          sourceSnapshotTmpDir = cmd.substring(sourceSnapshotTmpDirArgKey.length());
          continue;
        }

        final String peerSnapshotNameArgKey = "--peerSnapshotName=";
        if (cmd.startsWith(peerSnapshotNameArgKey)) {
          peerSnapshotName = cmd.substring(peerSnapshotNameArgKey.length());
          continue;
        }

        final String peerSnapshotTmpDirArgKey = "--peerSnapshotTmpDir=";
        if (cmd.startsWith(peerSnapshotTmpDirArgKey)) {
          peerSnapshotTmpDir = cmd.substring(peerSnapshotTmpDirArgKey.length());
          continue;
        }

        final String peerFSAddressArgKey = "--peerFSAddress=";
        if (cmd.startsWith(peerFSAddressArgKey)) {
          peerFSAddress = cmd.substring(peerFSAddressArgKey.length());
          continue;
        }

        final String peerHBaseRootAddressArgKey = "--peerHBaseRootAddress=";
        if (cmd.startsWith(peerHBaseRootAddressArgKey)) {
          peerHBaseRootAddress = cmd.substring(peerHBaseRootAddressArgKey.length());
          continue;
        }

        final String peerTableNameArgKey = "--peerTableName=";
        if (cmd.startsWith(peerTableNameArgKey)) {
          peerTableName = cmd.substring(peerTableNameArgKey.length());
          continue;
        }

        if (cmd.startsWith("--")) {
          printUsage("Invalid argument '" + cmd + "'");
          return false;
        }

        if (i == args.length-2) {
          if (isPeerQuorumAddress(cmd)) {
            peerQuorumAddress = cmd;
          } else {
            peerId = cmd;
          }
        }

        if (i == args.length-1) {
          tableName = cmd;
        }
      }

      if ((sourceSnapshotName != null && sourceSnapshotTmpDir == null)
          || (sourceSnapshotName == null && sourceSnapshotTmpDir != null)) {
        printUsage("Source snapshot name and snapshot temp location should be provided"
            + " to use snapshots in source cluster");
        return false;
      }

      if (peerSnapshotName != null || peerSnapshotTmpDir != null || peerFSAddress != null
          || peerHBaseRootAddress != null) {
        if (peerSnapshotName == null || peerSnapshotTmpDir == null || peerFSAddress == null
            || peerHBaseRootAddress == null) {
          printUsage(
            "Peer snapshot name, peer snapshot temp location, Peer HBase root address and  "
                + "peer FSAddress should be provided to use snapshots in peer cluster");
          return false;
        }
      }

      // This is to avoid making recompare calls to source/peer tables when snapshots are used
      if ((sourceSnapshotName != null || peerSnapshotName != null) && sleepMsBeforeReCompare > 0) {
        printUsage(
          "Using sleepMsBeforeReCompare along with snapshots is not allowed as snapshots are"
            + " immutable");
        return false;
      }

    } catch (Exception e) {
      e.printStackTrace();
      printUsage("Can't start because " + e.getMessage());
      return false;
    }
    return true;
  }

  private boolean isPeerQuorumAddress(String cmd) {
    try {
      ZKConfig.validateClusterKey(cmd);
    } catch (IOException e) {
      // not a quorum address
      return false;
    }
    return true;
  }

  /*
   * @param errorMsg Error message.  Can be null.
   */
  private static void printUsage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: verifyrep [--starttime=X]"
        + " [--endtime=Y] [--families=A] [--row-prefixes=B] [--delimiter=] [--recomparesleep=] "
        + "[--batch=] [--verbose] [--peerTableName=] [--sourceSnapshotName=P] "
        + "[--sourceSnapshotTmpDir=Q] [--peerSnapshotName=R] [--peerSnapshotTmpDir=S] "
        + "[--peerFSAddress=T] [--peerHBaseRootAddress=U] <peerid|peerQuorumAddress> <tablename>");
    System.err.println();
    System.err.println("Options:");
    System.err.println(" starttime    beginning of the time range");
    System.err.println("              without endtime means from starttime to forever");
    System.err.println(" endtime      end of the time range");
    System.err.println(" versions     number of cell versions to verify");
    System.err.println(" batch        batch count for scan, note that"
      + " result row counts will no longer be actual number of rows when you use this option");
    System.err.println(" raw          includes raw scan if given in options");
    System.err.println(" families     comma-separated list of families to copy");
    System.err.println(" row-prefixes comma-separated list of row key prefixes to filter on ");
    System.err.println(" delimiter    the delimiter used in display around rowkey");
    System.err.println(" recomparesleep   milliseconds to sleep before recompare row, " +
        "default value is 0 which disables the recompare.");
    System.err.println(" verbose      logs row keys of good rows");
    System.err.println(" peerTableName  Peer Table Name");
    System.err.println(" sourceSnapshotName  Source Snapshot Name");
    System.err.println(" sourceSnapshotTmpDir Tmp location to restore source table snapshot");
    System.err.println(" peerSnapshotName  Peer Snapshot Name");
    System.err.println(" peerSnapshotTmpDir Tmp location to restore peer table snapshot");
    System.err.println(" peerFSAddress      Peer cluster Hadoop FS address");
    System.err.println(" peerHBaseRootAddress  Peer cluster HBase root location");
    System.err.println();
    System.err.println("Args:");
    System.err.println(" peerid       Id of the peer used for verification,"
      + " must match the one given for replication");
    System.err.println(" peerQuorumAddress   quorumAdress of the peer used for verification. The "
      + "format is zk_quorum:zk_port:zk_hbase_path");
    System.err.println(" tablename    Name of the table to verify");
    System.err.println();
    System.err.println("Examples:");
    System.err.println(
      " To verify the data replicated from TestTable for a 1 hour window with peer #5 ");
    System.err.println(" $ hbase " +
        "org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication" +
        " --starttime=1265875194289 --endtime=1265878794289 5 TestTable ");
    System.err.println();
    System.err.println(
      " To verify the data in TestTable between the cluster runs VerifyReplication and cluster-b");
    System.err.println(" Assume quorum address for cluster-b is"
      + " cluster-b-1.example.com,cluster-b-2.example.com,cluster-b-3.example.com:2181:/cluster-b");
    System.err.println(
      " $ hbase org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication \\\n" +
        "     cluster-b-1.example.com,cluster-b-2.example.com,cluster-b-3.example.com:"
        + "2181:/cluster-b \\\n" +
        "     TestTable");
    System.err.println();
    System.err.println(
      " To verify the data in TestTable between the secured cluster runs VerifyReplication"
        + " and insecure cluster-b");
    System.err.println(
      " $ hbase org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication \\\n" +
        "     -D verifyrep.peer.hbase.security.authentication=simple \\\n" +
        "     cluster-b-1.example.com,cluster-b-2.example.com,cluster-b-3.example.com:"
        + "2181:/cluster-b \\\n" +
        "     TestTable");
    System.err.println();
    System.err.println(" To verify the data in TestTable between" +
      " the secured cluster runs VerifyReplication and secured cluster-b");
    System.err.println(" Assume cluster-b uses different kerberos principal, cluster-b/_HOST@E" +
      ", for master and regionserver kerberos principal from another cluster");
    System.err.println(
      " $ hbase org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication \\\n" +
        "     -D verifyrep.peer.hbase.regionserver.kerberos.principal="
        + "cluster-b/_HOST@EXAMPLE.COM \\\n" +
        "     -D verifyrep.peer.hbase.master.kerberos.principal=cluster-b/_HOST@EXAMPLE.COM \\\n" +
        "     cluster-b-1.example.com,cluster-b-2.example.com,cluster-b-3.example.com:"
        + "2181:/cluster-b \\\n" +
        "     TestTable");
    System.err.println();
    System.err.println(
      " To verify the data in TestTable between the insecure cluster runs VerifyReplication"
        + " and secured cluster-b");
    System.err.println(
      " $ hbase org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication \\\n" +
        "     -D verifyrep.peer.hbase.security.authentication=kerberos \\\n" +
        "     -D verifyrep.peer.hbase.regionserver.kerberos.principal="
        + "cluster-b/_HOST@EXAMPLE.COM \\\n" +
        "     -D verifyrep.peer.hbase.master.kerberos.principal=cluster-b/_HOST@EXAMPLE.COM \\\n" +
        "     cluster-b-1.example.com,cluster-b-2.example.com,cluster-b-3.example.com:"
        + "2181:/cluster-b \\\n" +
        "     TestTable");
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    Job job = createSubmittableJob(conf, args);
    if (job != null) {
      return job.waitForCompletion(true) ? 0 : 1;
    }
    return 1;
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(HBaseConfiguration.create(), new VerifyReplication(), args);
    System.exit(res);
  }
}
