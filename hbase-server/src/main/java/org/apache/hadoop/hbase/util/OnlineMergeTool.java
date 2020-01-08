/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility that can merge any two regions in the same table: adjacent,
 * overlapping or disjoint. It can also merge every regions, two by two.
 */
@InterfaceAudience.Private public class OnlineMergeTool extends Configured implements Tool {
  static final Logger LOG = LoggerFactory.getLogger(OnlineMergeTool.class);
  private final int COMPACTPAUSETIME = 180 * 1000;
  private final int DEFAULTMERGEPAUSETIME = 120 * 1000;
  private final String COMPACTIONATTRIBUTE = "MAJOR";
  private final long GB = 1024L * 1024L * 1024L;
  private final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  private final HBaseConfiguration conf;
  private volatile boolean isMetaTable;
  private volatile Connection connection;
  private volatile Admin admin;
  // Name of table
  private String tableName = null;
  // Name of region 1
  private String startRegion = null;
  // Name of region 2
  private String stopRegion = null;
  // Name of maxRegionSize
  private Long maxRegionSize = 0L;
  // Name of maxRegionCreateTime
  private String maxRegionCreateTime = null;
  // Name of numMaxMergePlans
  private String numMaxMergePlans = null;
  // Name of targetRegionCount
  private Long targetRegionCount = 0L;
  /**
   * print Execution Plan information
   */
  private boolean printExecutionPlan = true;
  /**
   * config merge pause time
   */
  private int mergePauseTime = 0;

  /**
   * default constructor
   */
  public OnlineMergeTool() throws IOException {
    this(new HBaseConfiguration());
  }

  /**
   * @param conf The current configuration.
   * @throws IOException If IO problem encountered
   */
  public OnlineMergeTool(HBaseConfiguration conf) throws IOException {
    super(conf);
    this.conf = conf;
    this.conf.setInt("hbase.client.retries.number", 3);
    this.conf.setInt("hbase.client.pause", 1000);
    this.connection = ConnectionFactory.createConnection(this.conf);
    this.admin = connection.getAdmin();
  }

  /**
   * Main program
   *
   * @param args The command line parameters.
   */
  public static void main(String[] args) {
    int status = 0;
    try {
      status = ToolRunner.run(new OnlineMergeTool(), args);
    } catch (Exception e) {
      LOG.error("exiting due to error", e);
      status = -1;
    }
    System.exit(status);
  }

  @Override public int run(String[] args) throws Exception {

    if (!doCommandLine(args)) {
      return -1;
    }

    isMetaTable =
      Bytes.compareTo(Bytes.toBytes(tableName), TableName.META_TABLE_NAME.getName()) == 0;
    // Verify file system is up.
    FileSystem fs = FileSystem.get(this.conf);              // get DFS handle
    LOG.info("Verifying that file system is available...");
    try {
      FSUtils.checkFileSystemAvailable(fs);
    } catch (IOException e) {
      LOG.error("File system is not available", e);
      return -1;
    }

    // Verify HBase is up
    LOG.info("Verifying that HBase is running...");
    try {
      admin.getClusterMetrics(EnumSet.of(ClusterMetrics.Option.HBASE_VERSION));
    } catch (MasterNotRunningException e) {
      LOG.error("HBase cluster must be on-line.");
      return -1;
    }

    List<RegionInfo> hRegionInfoList = admin.getRegions(TableName.valueOf(tableName));
    try {
      if (isMetaTable) {
        throw new Exception("Can't merge meta tables online");
      } else if (hRegionInfoList.size() <= targetRegionCount) {
        throw new Exception("Can't merge tables because regionCount=" + hRegionInfoList.size()
          + " less than targetRegionCount=" + targetRegionCount);
      } else if (printExecutionPlan) {
        executionPlan();
      } else {
        mergeRegions();
      }
      return 0;
    } catch (Exception e) {
      LOG.error("Merge failed", e);
      return -1;
    }
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

        final String tableNameKey = "--tableName=";
        if (cmd.startsWith(tableNameKey)) {
          tableName = cmd.substring(tableNameKey.length());
          continue;
        }

        final String startRegionKey = "--startRegion=";
        if (cmd.startsWith(startRegionKey)) {
          startRegion = cmd.substring(startRegionKey.length());
          continue;
        }

        final String stopRegionKey = "--stopRegion=";
        if (cmd.startsWith(stopRegionKey)) {
          stopRegion = cmd.substring(stopRegionKey.length());
          continue;
        }

        final String maxRegionSizeKey = "--maxRegionSize=";
        if (cmd.startsWith(maxRegionSizeKey)) {
          maxRegionSize = Long.parseLong(cmd.substring(maxRegionSizeKey.length())) * GB;
          continue;
        }

        final String maxRegionCreateTimeKey = "--maxRegionCreateTime=";
        if (cmd.startsWith(maxRegionCreateTimeKey)) {
          maxRegionCreateTime = cmd.substring(maxRegionCreateTimeKey.length());
          continue;
        }

        final String numMaxMergePlansKey = "--numMaxMergePlans=";
        if (cmd.startsWith(numMaxMergePlansKey)) {
          numMaxMergePlans = cmd.substring(numMaxMergePlansKey.length());
          continue;
        }

        final String targetRegionCountKey = "--targetRegionCount=";
        if (cmd.startsWith(targetRegionCountKey)) {
          targetRegionCount = Long.parseLong(cmd.substring(targetRegionCountKey.length()));
          continue;
        }

        final String printExecutionPlanKey = "--printExecutionPlan=";
        if (cmd.startsWith(printExecutionPlanKey)) {
          printExecutionPlan = Boolean.parseBoolean(cmd.substring(printExecutionPlanKey.length()));
          continue;
        }

        final String mergePauseTimekey = "--configMergePauseTime=";
        if (cmd.startsWith(mergePauseTimekey)) {
          mergePauseTime = Integer.parseInt(cmd.substring(mergePauseTimekey.length()));
          continue;
        }
      }

      if (null == tableName || tableName.isEmpty()) {
        printUsage("table name must be not null");
        return false;
      }

      if (null == maxRegionSize || tableName.isEmpty()) {
        printUsage("table name must be not null");
        return false;
      }

      if (startRegion != null && stopRegion != null) {
        if (notInTable(Bytes.toBytes(tableName), Bytes.toBytes(startRegion)) || notInTable(
          Bytes.toBytes(tableName), Bytes.toBytes(stopRegion))) {
          LOG.error(
            "Can't merge region not in table or region is null startRegion is " + startRegion
              + " stopRegion is " + stopRegion);
          return false;
        } else if (startRegion.equals(stopRegion)) {
          LOG.error("Can't merge a region with itself");
          return false;
        }
      }
      if (startRegion != null) {
        if (null == stopRegion) {
          printUsage(
            "The startRegion and the stopRegion must be used in pairs stopRegion=" + stopRegion);
          return false;
        }
      }
      if (stopRegion != null) {
        if (null == startRegion) {
          printUsage(
            "The startRegion and the stopRegion must be used in pairs startRegion=" + startRegion);
          return false;
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
      printUsage("Can't start because " + e.getMessage());
      return false;
    }
    return true;
  }

  private List<Pair<byte[], byte[]>> executionPlan() throws IOException, ParseException {
    List<RegionInfo> hris = getListRegionInfo(tableName, startRegion, stopRegion);
    if (hris.size() < 2) {
      throw new IOException("The table doesn't have 2 or more regions region count=" + hris.size());
    }
    RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(tableName));
    RegionSizeCalculator regionSizeCalculator = new RegionSizeCalculator(regionLocator, admin);
    List<Pair<byte[], byte[]>> mergePlans = new ArrayList<Pair<byte[], byte[]>>();
    for (int i = 0; i < hris.size() - 1; i += 2) {
      RegionInfo regionInfo1 = hris.get(i);
      RegionInfo regionInfo2 = hris.get(i + 1);
      if (regionInfo1.isOffline() || regionInfo1.isSplit() || regionInfo2.isOffline() || regionInfo2
        .isSplit()) {
        LOG.info("Skip Region split or offline region1=" + regionInfo1.getRegionNameAsString()
          + " region2=" + regionInfo2.getRegionNameAsString());
        continue;
      }
      if (null != maxRegionCreateTime) {
        long time2Timestamp = DATE_FORMAT.parse(maxRegionCreateTime).getTime();
        if (regionInfo1.getRegionId() > time2Timestamp
          || regionInfo2.getRegionId() > time2Timestamp) {
          StringBuffer mesg = new StringBuffer();
          mesg.append("Skip Region timestamp region1=");
          mesg.append(Bytes.toString(regionInfo1.getEncodedNameAsBytes()));
          mesg.append(" region1Timestamp=").append(regionInfo1.getRegionId());
          mesg.append(" > maxRegionCreateTime=").append(time2Timestamp);
          mesg.append(" or region2=").append(Bytes.toString(regionInfo2.getEncodedNameAsBytes()));
          mesg.append(" region1Timestamp=").append(regionInfo2.getRegionId());
          mesg.append(" > maxRegionCreateTime=").append(time2Timestamp);
          LOG.info(mesg.toString());
          continue;
        }
      }
      long regionSize = regionSizeCalculator.getRegionSize(regionInfo1.getRegionName());
      long regionSize_next = regionSizeCalculator.getRegionSize(regionInfo2.getRegionName());
      if (regionSize > maxRegionSize || regionSize_next > maxRegionSize) {
        StringBuilder mesg = new StringBuilder();
        mesg.append("Skip Region size region1=");
        mesg.append(Bytes.toString(regionInfo1.getEncodedNameAsBytes()));
        mesg.append(" region1Size=").append(regionSize);
        mesg.append(" > maxRegionSize=").append(maxRegionSize);
        mesg.append(" or region2=").append(Bytes.toString(regionInfo2.getEncodedNameAsBytes()));
        mesg.append(" region2Size=").append(regionSize_next);
        mesg.append(" > maxRegionSize=").append(maxRegionSize);

        LOG.info(mesg.toString());
        continue;
      }
      Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>();
      pair.setFirst(regionInfo1.getEncodedNameAsBytes());
      pair.setSecond(regionInfo2.getEncodedNameAsBytes());
      mergePlans.add(pair);
      StringBuilder mesg = new StringBuilder();
      mesg.append("Print merge plans region1=");
      mesg.append(Bytes.toString(regionInfo1.getEncodedNameAsBytes()));
      mesg.append(" region1=").append(regionSize / GB);
      mesg.append("G region2=");
      mesg.append(Bytes.toString(regionInfo2.getEncodedNameAsBytes()));
      mesg.append(" region2=").append(regionSize_next / GB).append("G");
      LOG.info(mesg.toString());
    }
    return mergePlans;
  }

  /*
   * Merges two regions from a user table.
   */
  private void mergeRegions() throws IOException, InterruptedException, ParseException {
    List<Pair<byte[], byte[]>> mergePlans = executionPlan();
    if (mergePlans.size() < 1) {
      printCompletedMesg();
      return;
    }
    if (null == numMaxMergePlans) {
      for (Pair<byte[], byte[]> region : mergePlans) {
        admin.mergeRegionsAsync(region.getFirst(), region.getSecond(), false);
        LOG.info("Merging regions " + Bytes.toString(region.getFirst()) + " and " + Bytes
          .toString(region.getSecond()) + " in table " + tableName);
      }
      Thread.sleep(mergePauseTime > 0 ? mergePauseTime : DEFAULTMERGEPAUSETIME);
      if (maxRegionSize != 0) {
        admin.compact(TableName.valueOf(tableName));
        LOG.info("Table=" + tableName + " is runing compact");
        runCompaction(tableName);
      }
    } else {
      long numMaxMergePlans2Long = Long.parseLong(numMaxMergePlans);
      for (int i = 0; i < mergePlans.size(); i++) {
        admin.mergeRegionsAsync(mergePlans.get(i).getFirst(), mergePlans.get(i).getSecond(), false);
        LOG.info("Merging regions " + Bytes.toString(mergePlans.get(i).getFirst()) + " and " + Bytes
          .toString(mergePlans.get(i).getSecond()) + " in table " + tableName);
        if (i + 1 % numMaxMergePlans2Long == 0) {
          Thread.sleep(mergePauseTime > 0 ? mergePauseTime : DEFAULTMERGEPAUSETIME);
          if (maxRegionSize != 0) {
            admin.compact(TableName.valueOf(tableName));
            LOG.info("Table=" + tableName + " is runing compact");
            runCompaction(tableName);
          }
        }
      }
      Thread.sleep(mergePauseTime > 0 ? mergePauseTime : DEFAULTMERGEPAUSETIME);
      if (maxRegionSize != 0) {
        admin.compact(TableName.valueOf(tableName));
        LOG.info("Table=" + tableName + " is runing compact");
        runCompaction(tableName);
      }
    }
    List<RegionInfo> hRegionInfoList = admin.getRegions(TableName.valueOf(tableName));
    if (hRegionInfoList.size() <= targetRegionCount
      || getListRegionInfo(tableName, startRegion, stopRegion).size() < 2) {
      printCompletedMesg();
    } else {
      mergeRegions();
    }
  }

  /**
   * print merge completed Mesg
   */
  private void printCompletedMesg() {
    StringBuilder mesg = new StringBuilder();
    mesg.append("Merge completed table=");
    mesg.append(tableName);
    mesg.append(" startRegion=");
    mesg.append(startRegion);
    mesg.append(" stopRegion=");
    mesg.append(stopRegion);
    mesg.append(" maxRegionSize=");
    mesg.append(maxRegionSize / GB).append("G");
    mesg.append(" maxRegionCreateTime=").append(maxRegionCreateTime);
    mesg.append(" numMaxMergePlans=");
    mesg.append(numMaxMergePlans);
    mesg.append(" targetRegionCount=");
    mesg.append(targetRegionCount);
    LOG.info(mesg.toString());
  }

  /**
   * Get the list of a HRIs in a table
   *
   * @return list of hris
   * @throws IOException If IO problem encountered
   */
  List<RegionInfo> getListRegionInfo(String tableName, String startRegion, String stopRegion)
    throws IOException {
    boolean isAdd = false;
    List<RegionInfo> hris = new ArrayList<RegionInfo>();
    List<RegionInfo> tableRegions = this.admin.getRegions(TableName.valueOf(tableName));
    for (RegionInfo hri : tableRegions) {
      if (null == startRegion && null == stopRegion) {
        hris.add(hri);
        LOG.info("Add legitimate range resgion=" + hri.getRegionNameAsString());

      } else if (null != startRegion && null != stopRegion) {
        if (hri.getRegionNameAsString().equals(startRegion)) {
          LOG.info("Open interval startRegion=" + hri.getRegionNameAsString());
          isAdd = true;
          continue;
        }
        if (hri.getRegionNameAsString().equals(stopRegion)) {
          LOG.info("Open interval stopRegion=" + hri.getRegionNameAsString());
          isAdd = false;
          break;
        }
        if (isAdd) {
          hris.add(hri);
          LOG.info("Add legitimate range resgion=" + hri.getRegionNameAsString());
        }
      }
    }
    return hris;
  }

  /**
   * Waiting for compaction complete
   *
   * @param tableName table name
   * @throws IOException          If IO problem encountered
   * @throws InterruptedException If Interrupted problem encountered
   */
  private void runCompaction(String tableName) throws IOException, InterruptedException {
    while (true) {
      long startTime = System.currentTimeMillis();
      String compactionState =
        this.admin.getCompactionState(TableName.valueOf(tableName)).toString();
      if (!COMPACTIONATTRIBUTE.equals(compactionState)) {
        LOG.info("Table=" + tableName + " compationState=" + compactionState + " compact complete");
        break;
      }
      Thread.sleep(COMPACTPAUSETIME);
      long waitTime = (System.currentTimeMillis() - startTime) / 1000;
      LOG.info("Table=" + tableName + " compationState=" + compactionState + " the waiting time "
        + waitTime + "seconds");
    }
  }

  private boolean notInTable(final byte[] tn, final byte[] rn) {
    if (WritableComparator.compareBytes(tn, 0, tn.length, rn, 0, tn.length) != 0) {
      LOG.error("Region " + Bytes.toString(rn) + " does not belong to table " + Bytes.toString(tn));
      return true;
    }
    return false;
  }

  /*
   * @param errorMsg Error message.  Can be null.
   */
  private void printUsage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: bin/hbase onlinemerge [--tableName=] "
      + "[--startRegion=] [--stopRegion=] [--maxRegionSize=] "
      + "[--maxRegionCreateTime=] [--numMaxMergePlans=] "
      + "[--targetRegionCount=] [--printExecutionPlan=] [--configMergePauseTime=]\n");
    System.err.println("Options:");
    System.err.println("--h or --h              print help");
    System.err.println("--tableName             table name must be not null");
    System.err.println("--startRegion           start region");
    System.err.println("--stopRegion            stop region");
    System.err.println("--maxRegionSize         max region size Unit GB");
    System.err.println("--maxRegionCreateTime   max Region Create Time yyyy/MM/dd HH:mm:ss");
    System.err.println("--numMaxMergePlans      num MaxMerge Plans");
    System.err.println("--targetRegionCount     target Region Count");
    System.err.println("--configMergePauseTime  config Merge Pause Time In milliseconds");
    System.err.println("--printExecutionPlan    Value default is true print execution plans "
      + "false is execution merge\n");
    System.err.println("Examples:");
    System.err.println("bin/hbase onlinemerge --tableName=test:test1 "
      + "--startRegion=test:test1,,1576835912332.01d0d6c2b41e204104524d9aec6074fb. "
      + "--stopRegion=test:test1,bbbbbbbb,1573044786980.0c9b5bd93f3b19eb9bd1a1011ddff66f. "
      + "--maxRegionSize=0 --maxRegionCreateTime=yyyy/MM/dd HH:mm:ss "
      + "--numMaxMergePlans=2 --targetRegionCount=4 --printExecutionPlan=false");
  }
}
