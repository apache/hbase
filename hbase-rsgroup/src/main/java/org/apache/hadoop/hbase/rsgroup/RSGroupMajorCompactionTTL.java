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

package org.apache.hadoop.hbase.rsgroup;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.compaction.MajorCompactorTTL;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.DefaultParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Option;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;
import org.apache.hbase.thirdparty.org.apache.commons.cli.ParseException;

/**
 * This script takes an rsgroup as argument and compacts part/all of regions of that table
 * based on the table's TTL.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class RSGroupMajorCompactionTTL extends MajorCompactorTTL {

  private static final Logger LOG = LoggerFactory.getLogger(RSGroupMajorCompactionTTL.class);

  @InterfaceAudience.Private
  RSGroupMajorCompactionTTL() {
    super();
  }

  public int compactTTLRegionsOnGroup(Configuration conf, String rsgroup, int concurrency,
      long sleep, int numServers, int numRegions, boolean dryRun, boolean skipWait)
      throws Exception {

    Connection conn = ConnectionFactory.createConnection(conf);
    RSGroupAdmin rsGroupAdmin = new RSGroupAdminClient(conn);

    RSGroupInfo rsGroupInfo = rsGroupAdmin.getRSGroupInfo(rsgroup);
    if (rsGroupInfo == null) {
      LOG.error("Invalid rsgroup specified: " + rsgroup);
      throw new IllegalArgumentException("Invalid rsgroup specified: " + rsgroup);
    }

    for (TableName tableName : rsGroupInfo.getTables()) {
      int status = compactRegionsTTLOnTable(conf, tableName.getNameAsString(), concurrency, sleep,
          numServers, numRegions, dryRun, skipWait);
      if (status != 0) {
        LOG.error("Failed to compact table: " + tableName);
        return status;
      }
    }
    return 0;
  }

  protected Options getOptions() {
    Options options = getCommonOptions();

    options.addOption(
        Option.builder("rsgroup")
            .required()
            .desc("Tables of rsgroup to be compacted")
            .hasArg()
            .build()
    );

    return options;
  }

  @Override
  public int run(String[] args) throws Exception {
    Options options = getOptions();

    final CommandLineParser cmdLineParser = new DefaultParser();
    CommandLine commandLine;
    try {
      commandLine = cmdLineParser.parse(options, args);
    } catch (ParseException parseException) {
      System.out.println(
          "ERROR: Unable to parse command-line arguments " + Arrays.toString(args) + " due to: "
              + parseException);
      printUsage(options);
      return -1;
    }
    if (commandLine == null) {
      System.out.println("ERROR: Failed parse, empty commandLine; " + Arrays.toString(args));
      printUsage(options);
      return -1;
    }

    String rsgroup = commandLine.getOptionValue("rsgroup");
    int numServers = Integer.parseInt(commandLine.getOptionValue("numservers", "-1"));
    int numRegions = Integer.parseInt(commandLine.getOptionValue("numregions", "-1"));
    int concurrency = Integer.parseInt(commandLine.getOptionValue("servers", "1"));
    long sleep = Long.parseLong(commandLine.getOptionValue("sleep", Long.toString(30000)));
    boolean dryRun = commandLine.hasOption("dryRun");
    boolean skipWait = commandLine.hasOption("skipWait");
    Configuration conf = getConf();

    return compactTTLRegionsOnGroup(conf, rsgroup, concurrency, sleep, numServers, numRegions,
        dryRun, skipWait);
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(HBaseConfiguration.create(), new RSGroupMajorCompactionTTL(), args);
  }
}
