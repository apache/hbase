/**
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

/**
 * Tool used to copy a table to another one which can be on a different setup.
 * It is also configurable with a start and time as well as a specification
 * of the region server implementation if different from the local cluster.
 */
public class CopyTable {

  final static String NAME = "Copy Table";
  private final String rsClass;
  private final String rsImpl;
  private final long startTime;
  private final long endTime;
  private final String tableName;
  private final String newTableName;
  private final String peerAddress;
  private final String[] families;


  public CopyTable(String rsClass, String rsImpl, long startTime, long endTime,
      String tableName, String newTableName, String zookeeperQuorum,
      String[] families) {
    this.rsClass = rsClass;
    this.rsImpl = rsImpl;
    this.startTime = startTime;
    this.endTime = endTime;
    this.tableName = tableName;
    this.newTableName = newTableName;
    this.peerAddress = zookeeperQuorum;
    this.families = families;
  }

  /**
   * Sets up the actual job.
   *
   * @param conf
   *          The current configuration.
   * @param args
   *          The command line parameters.
   * @return The newly created job.
   * @throws IOException
   *           When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf, String[] args)
      throws IOException {
    Job job = null;
    CopyTable cpTbl = null;
    try {
      cpTbl = buildCopyTableFromArguments(conf, args);
    } catch (ParseException e) {
      e.printStackTrace();
      return null;
    }
    if (cpTbl != null) {
      job = new Job(conf, NAME + "_" + cpTbl.tableName);
      job.setJarByClass(CopyTable.class);
      Scan scan = new Scan();
      if (cpTbl.startTime != 0) {
        scan.setTimeRange(cpTbl.startTime,
            cpTbl.endTime == 0 ? HConstants.LATEST_TIMESTAMP : cpTbl.endTime);
      }
      if (cpTbl.families != null) {
        for (String fam : cpTbl.families) {
          scan.addFamily(Bytes.toBytes(fam));
        }
      }
      TableMapReduceUtil.initTableMapperJob(cpTbl.tableName, scan,
          Import.Importer.class, null, null, job);
      TableMapReduceUtil.initTableReducerJob(
          cpTbl.newTableName == null ? cpTbl.tableName : cpTbl.newTableName,
          null, job, null, cpTbl.peerAddress, cpTbl.rsClass, cpTbl.rsImpl);
      job.setNumReduceTasks(0);
    } else {
      System.out.println("CopyTable object is null, exiting!");
      return null;
    }
    return job;
  }

  private static void printHelp(Options opt) {
    String example = "To copy 'TestTable' to a cluster that uses replication for a 1 hour window:\n $ bin/hbase "
        + "org.apache.hadoop.hbase.mapreduce.CopyTable --rs.class=org.apache.hadoop.hbase.ipc.ReplicationRegionInterface "
        + "--rs.impl=org.apache.hadoop.hbase.regionserver.replication.ReplicationRegionServer --starttime=1265875194289 --endtime=1265878794289 "
        + "--peer.adr=server1,server2,server3:/hbase TestTable ";
    new HelpFormatter().printHelp(
        "Job needs to be run on the source cluster \n CopyTable < tablename | -h > [--rs.class=CLASS] "
            + "[--rs.impl=IMPL] [--starttime=X] [--endtime=Y] "
            + "[--new.name=NEW] [--peer.adr=ADR] [--D]", "", opt,
        "Example: " + example);
  }

  private static Options buildOptions() {
    Options opt = new Options();
    opt.addOption("rs.class", true,
        "hbase.regionserver.class of the destination cluster");
    opt.addOption("rs.impl", true,
        "hbase.regionserver.impl of the destination cluster");
    opt.addOption(
        "starttime",
        true,
        "beginning of the time range, don't specify if you want to copy [-infinity, end)");
    opt.addOption(
        "endtime",
        true,
        "end of the time range, don't specify if you want to copy the data [start, infinity)");
    opt.addOption("new.name", true,
        "name of the new table on the destination cluster");
    opt.addOption("peer.adr", true,
        "zookeeper.quorum of the destination cluster");
    opt.addOption("families", true, "comma-seperated list of families to copy");
    opt.addOption("tablename", true,
        "name of the table that we are copying from the source cluster");
    opt.addOption("D", true, "configuration override, provide arguments in format conf1=x, conf2=y, etc.");
    opt.addOption("-h", false, "help");
    return opt;
  }

  /**
   * Construct CopyTable object from the arguments from command line
   */
  private static CopyTable buildCopyTableFromArguments(Configuration conf,
      final String[] args) throws ParseException {
    Options opt = buildOptions();
    CommandLine cmd = new GnuParser().parse(opt, args);
    String rsClass = cmd.getOptionValue("rs.class");
    String rsImpl = cmd.getOptionValue("rs.impl");
    String startTimeString = cmd.getOptionValue("starttime");
    long startTime = startTimeString == null ? 0 : Integer
        .valueOf(startTimeString);
    String endTimeString = cmd.getOptionValue("endtime");
    long endTime = endTimeString == null ? 0 : Integer
        .valueOf(endTimeString);
    String newTableName = cmd.getOptionValue("new.name");
    String zookeeperQuorum = cmd.getOptionValue("peer.adr");
    String[] families = cmd.getOptionValues("families");
    String tableName = cmd.getOptionValue("tablename");
    if (tableName == null && zookeeperQuorum == null) {
      printHelp(opt);
      throw new ParseException(
          "tableName OR zookeperQuorum (peer.adr) must be specified");
    }
    if (cmd.hasOption("D")) {
      for (String confOpt : cmd.getOptionValues("D")) {
        String[] kv = confOpt.split("=", 2);
        if (kv.length == 2) {
          conf.set(kv[0], kv[1]);
          System.out.println("-D configuration override: " + kv[0] + "="
              + kv[1]);
        } else {
          throw new ParseException("-D option format invalid: " + confOpt);
        }
      }
    }
    return new CopyTable(rsClass, rsImpl, startTime, endTime, tableName,
        newTableName, zookeeperQuorum, families);
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Job job = createSubmittableJob(conf, args);
    if (job != null) {
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
  }
}
