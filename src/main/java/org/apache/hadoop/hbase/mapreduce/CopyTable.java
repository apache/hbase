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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Tool used to copy a table to another one which can be on a different setup.
 * It is also configurable with a start and time as well as a specification
 * of the region server implementation if different from the local cluster.
 */
public class CopyTable extends Configured implements Tool {

  final static String NAME = "copytable";
  static long startTime = 0;
  static long endTime = 0;
  static int versions = -1;
  static String tableName = null;
  static String startRow = null;
  static String stopRow = null;
  static String newTableName = null;
  static String peerAddress = null;
  static String families = null;
  static boolean allCells = false;
  
  public CopyTable(Configuration conf) {
    super(conf);
  }
  /**
   * Sets up the actual job.
   *
   * @param conf  The current configuration.
   * @param args  The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf, String[] args)
  throws IOException {
    if (!doCommandLine(args)) {
      return null;
    }
    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJarByClass(CopyTable.class);
    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    if (startTime != 0) {
      scan.setTimeRange(startTime,
          endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
    }
    if (allCells) {
      scan.setRaw(true);
    }
    if (versions >= 0) {
      scan.setMaxVersions(versions);
    }
    
    if (startRow != null) {
      scan.setStartRow(Bytes.toBytes(startRow));
    }
    
    if (stopRow != null) {
      scan.setStopRow(Bytes.toBytes(stopRow));
    }
    
    if(families != null) {
      String[] fams = families.split(",");
      Map<String,String> cfRenameMap = new HashMap<String,String>();
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
    TableMapReduceUtil.initTableMapperJob(tableName, scan,
        Import.Importer.class, null, null, job);
    TableMapReduceUtil.initTableReducerJob(
        newTableName == null ? tableName : newTableName, null, job,
        null, peerAddress, null, null);
    job.setNumReduceTasks(0);
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
        "[--new.name=NEW] [--peer.adr=ADR] <tablename>");
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
    System.err.println("              hbase.zookeeer.quorum:hbase.zookeeper.client.port:zookeeper.znode.parent");
    System.err.println(" families     comma-separated list of families to copy");
    System.err.println("              To copy from cf1 to cf2, give sourceCfName:destCfName. ");
    System.err.println("              To keep the same name, just give \"cfName\"");
    System.err.println(" all.cells    also copy delete markers and deleted cells");
    System.err.println();
    System.err.println("Args:");
    System.err.println(" tablename    Name of the table to copy");
    System.err.println();
    System.err.println("Examples:");
    System.err.println(" To copy 'TestTable' to a cluster that uses replication for a 1 hour window:");
    System.err.println(" $ bin/hbase " +
        "org.apache.hadoop.hbase.mapreduce.CopyTable --starttime=1265875194289 --endtime=1265878794289 " +
        "--peer.adr=server1,server2,server3:2181:/hbase --families=myOldCf:myNewCf,cf2,cf3 TestTable ");
    System.err.println("For performance consider the following general options:\n"
        + "-Dhbase.client.scanner.caching=100\n"
        + "-Dmapred.map.tasks.speculative.execution=false");
  }

  private static boolean doCommandLine(final String[] args) {
    // Process command-line args. TODO: Better cmd-line processing
    // (but hopefully something not as painful as cli options).
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

        final String versionsArgKey = "--versions=";
        if (cmd.startsWith(versionsArgKey)) {
          versions = Integer.parseInt(cmd.substring(versionsArgKey.length()));
          continue;
        }

        final String newNameArgKey = "--new.name=";
        if (cmd.startsWith(newNameArgKey)) {
          newTableName = cmd.substring(newNameArgKey.length());
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

        if (i == args.length-1) {
          tableName = cmd;
        } else {
          printUsage("Invalid argument '" + cmd + "'" );
          return false;
        }
      }
      if (newTableName == null && peerAddress == null) {
        printUsage("At least a new table name or a " +
            "peer address must be specified");
        return false;
      }
      if (startTime > endTime) {
        printUsage("Invalid time range filter: starttime=" + startTime + " >  endtime=" + endTime);
        return false;
      }
    } catch (Exception e) {
      e.printStackTrace();
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
    int ret = ToolRunner.run(new CopyTable(HBaseConfiguration.create()), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
    Job job = createSubmittableJob(getConf(), otherArgs);
    if (job == null) return 1;
    return job.waitForCompletion(true) ? 0 : 1;
  }
}
